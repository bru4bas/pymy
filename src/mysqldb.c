
#include <mysql.h>
#include <Python.h>
#include <stdlib.h>
#include "structmember.h"

typedef struct {
   PyObject_HEAD
   MYSQL *con;                               // MySQL connection handler
} Connection_Object;

typedef struct {
   PyObject_HEAD
   PyObject *fields;                         // Tuple with field names
   MYSQL_RES *res;                           // MySQL stored result 
   MYSQL_ROW last;                           // last read row from result
   unsigned row;                             // last row index
   unsigned num_rows;                        // total number of rows in result
} Result_Object;

void 
Result_dealloc(PyObject *self)
{
   Result_Object *s = (Result_Object*)self;
   mysql_free_result(s->res);
   Py_XDECREF(s->fields);
   Py_TYPE(self)->tp_free(self);
}

PyObject *
Result_getitem(PyObject *self, Py_ssize_t index)
{
   Result_Object *s = (Result_Object*)self;
   if(index != s->row) {
      /*
       * Read a different row from MySQL stored result.
       */
      if(index >= s->num_rows) {
         /*
          * Out of range
          */
         PyErr_SetString(PyExc_IndexError, "Row out of range");
         return NULL;
      }

      mysql_data_seek(s->res, index);
      s->row = index;
      s->last = mysql_fetch_row(s->res);
   }

   /*
    * Create a dictionary with the fields and values.
    */
   PyObject *item = PyDict_New();
   if(item == NULL) return NULL;

   unsigned i;
   unsigned n = PyTuple_Size(s->fields);
   for(i=0; i<n; i++) {
      PyObject *key = PyTuple_GetItem(s->fields, i);
      PyObject *value;
      char *cvalue = s->last[i];
      if(cvalue == NULL) {
         /*
          * No value or NULL translated into None
          */
         value = Py_None;
         Py_INCREF(Py_None);
      } else {
         value = PyString_FromString(cvalue);
         if(value == NULL) {
            Py_DECREF(item);
            return NULL;
         }
      }
      if(PyDict_SetItem(item, key, value) < 0) {
         Py_DECREF(item);
         return NULL;
      }
   }

   return item;   
}

PyObject *
Result_getcolumn(PyObject *self, PyObject *args)
{
   Result_Object *s = (Result_Object*)self;
   char *field;

   if(!PyArg_ParseTuple(args, "s", &field)) return NULL;

   /*
    * Find the desired field into the fields list
    */
   unsigned i;
   unsigned n = PyTuple_Size(s->fields);
   for(i=0; i<n; i++) {
      char *f = PyString_AS_STRING(PyTuple_GET_ITEM(s->fields, i));
      if(strcmp(field, f) == 0) break;
   }
   if(i >= n) {
      PyErr_SetString(PyExc_IndexError, "Unknown field");
      return NULL;
   }

   PyObject *col = PyTuple_New(s->num_rows);
   if(col == NULL) return NULL;

   unsigned j;
   mysql_data_seek(s->res, 0);
   for(j=0; j<s->num_rows; j++) {
      s->row = j;
      s->last = mysql_fetch_row(s->res);
      char *cvalue = s->last[i];
      if(cvalue == NULL) {
         Py_INCREF(Py_None);
         PyTuple_SetItem(col, j, Py_None); 
      } else {
         PyTuple_SetItem(col, j, PyString_FromString(cvalue));
      }
   }

   return col;
}

Py_ssize_t 
Result_getsize(PyObject *self)
{
   Result_Object *s = (Result_Object*)self;
   return s->num_rows;
}

/*
 * Result can work as a (imutable) sequence.
 */
static PySequenceMethods Result_as_sequence = {
   Result_getsize,                                 /* sq_length */
   0,                                              /* sq_concat */
   0,                                              /* sq_repeat */
   Result_getitem,                                 /* sq_item */
   0,                                              /* sq_ass_item */
   0,                                              /* sq_contains */
   0,                                              /* sq_inplace_concat */
   0                                               /* sq_inplace_repeat */
};

/*
 * Members for Result Object
 */
static PyMemberDef Result_members[] = {
   { "fields", T_OBJECT, offsetof(Result_Object, fields), READONLY, "Field names" },
   { NULL }
};

/*
 * C Methods for Result Object
 */
static PyMethodDef Result_methods[] = {
   { "column", (PyCFunction)Result_getcolumn, METH_VARARGS, "Tuple with all values of the column" },
   { NULL }
};

/*
 * Result Type definition for Python
 */
static PyTypeObject Result_Type = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "mysqldb.Result",                               /* tp_name */
   sizeof(Result_Object),                          /* tp_basicsize */
   0,                                              /* tp_itemsize */
   (destructor)Result_dealloc,                     /* tp_dealloc */
   0,                                              /* tp_print */
   0,                                              /* tp_getattr */
   0,                                              /* tp_setattr */
   0,                                              /* tp_compare */
   0,                                              /* tp_repr */
   0,                                              /* tp_as_number */
   &Result_as_sequence,                            /* tp_as_sequence */
   0,                                              /* tp_as_mapping */
   0,                                              /* tp_hash */
   0,                                              /* tp_call */
   0,                                              /* tp_str */
   0,                                              /* tp_getattro */
   0,                                              /* tp_setattro */
   0,                                              /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,       /* tp_flags */
   "MySQL Query Results",                          /* tp_doc */
   0,                                              /* tp_traverse */
   0,                                              /* tp_clear */
   0,                                              /* tp_richcompare */
   0,                                              /* tp_weaklistoffset */
   0,                                              /* tp_iter */
   0,                                              /* tp_iternext */
   Result_methods,                                 /* tp_methods */
   Result_members,                                 /* tp_members */
   0,                                              /* tp_getset */
   0,                                              /* tp_base */
   0,                                              /* tp_dict */
   0,                                              /* tp_descr_get */
   0,                                              /* tp_descr_set */
   0,                                              /* tp_dictoffset */
   0,                                              /* tp_init */
   0,                                              /* tp_alloc */
   0,                                              /* tp_new */
};

static PyObject *
Connection_new(PyTypeObject *type, PyObject *args, PyObject *keywords)
{
   Connection_Object *self = (Connection_Object*)type->tp_alloc(type, 0);
   if(self != NULL) {
      self->con = mysql_init(NULL);
      if(self->con == NULL) {
         PyErr_SetString(PyExc_RuntimeError, mysql_error(self->con));
         Py_XDECREF(self);
         return NULL;
      }
   }

   return (PyObject *)self;
}

void
Connection_dealloc(PyObject *self)
{
   Connection_Object *s = (Connection_Object*)self;
   if(s->con != NULL)
      mysql_close(s->con);
   Py_TYPE(self)->tp_free(self);
}

static int
Connection_init(Connection_Object *self, PyObject *args, PyObject *keywords)
{
   static char *keys[] = { "database", "host", "user", "password", NULL };
   char *db;
   char *host = "localhost";
   char *user = "root";
   char *pwd = "";
   
   if(!PyArg_ParseTupleAndKeywords(args, keywords, "s|sss", keys, &db, &host, &user, &pwd))
      return -1;
   
   if(mysql_real_connect(self->con, host, user, pwd, db, 0, NULL, 0) == NULL) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(self->con));
      return -1;
   }

   return 0;
}

static PyObject *
Connection_query(PyObject *self, PyObject *args)
{
   char *query;
   Connection_Object *s = (Connection_Object*)self;

   /*
    * Send query to MySQL server
    */
   if(!PyArg_ParseTuple(args, "s", &query)) return NULL;
   if(mysql_query(s->con, query)) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(s->con));
      return NULL;
   }

   /*
    * Read back results (store to memory).
    */
   MYSQL_RES *res = mysql_store_result(s->con);
   if(res == NULL) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(s->con));
      return NULL;
   }

   /*
    * Count results.
    */
   unsigned rows = mysql_num_rows(res);
   unsigned fields = mysql_field_count(s->con);
   if((rows == 0) || (fields == 0)) {
      /*
       * No results.
       */
      mysql_free_result(res);
      Py_RETURN_NONE;
   }

   /*
    * Create new Result object.
    */
   Result_Object *result = PyObject_New(Result_Object, &Result_Type);
   if(result == NULL) {
      mysql_free_result(res);
      return NULL;
   }

   result->num_rows = rows;
   result->res = res;
   result->row = -1;
   result->last = NULL;

   /*
    * Create new Tuple with fields' names
    */
   result->fields = PyTuple_New(fields);
   unsigned i;
   for(i=0; ;i++) {
      MYSQL_FIELD *f = mysql_fetch_field(res);
      if(f == NULL) break;
      PyTuple_SetItem(result->fields, i, PyString_FromString(f->name));
   }

   return (PyObject*)result;
}

static PyObject *
Connection_exec(PyObject *self, PyObject *args)
{
   char *query;
   Connection_Object *s = (Connection_Object*)self;

   /*
    * Send query to MySQL server
    */
   if(!PyArg_ParseTuple(args, "s", &query)) return NULL;
   if(mysql_query(s->con, query)) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(s->con));
      return NULL;
   }

   /*
    * Return number of affected rows.
    */
   return PyInt_FromLong(mysql_affected_rows(s->con));
}

/*
 * C Methods for Connection Object
 */
static PyMethodDef Connection_methods[] = {
   { "query", (PyCFunction)Connection_query, METH_VARARGS, "Execute a SQL query and return a result (None) if no result" },
   { "execute", (PyCFunction)Connection_exec, METH_VARARGS, "Execute a SQL command and return the number of affected rows" },
   { NULL }
};

/*
 * Connection Type definition for Python
 */
static PyTypeObject Connection_Type = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "mysqldb.Connection",                           /* tp_name */
   sizeof(Connection_Object),                      /* tp_basicsize */
   0,                                              /* tp_itemsize */
   (destructor)Connection_dealloc,                 /* tp_dealloc */
   0,                                              /* tp_print */
   0,                                              /* tp_getattr */
   0,                                              /* tp_setattr */
   0,                                              /* tp_compare */
   0,                                              /* tp_repr */
   0,                                              /* tp_as_number */
   0,                                              /* tp_as_sequence */
   0,                                              /* tp_as_mapping */
   0,                                              /* tp_hash */
   0,                                              /* tp_call */
   0,                                              /* tp_str */
   0,                                              /* tp_getattro */
   0,                                              /* tp_setattro */
   0,                                              /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,       /* tp_flags */
   "MySQL Connection abstraction",                 /* tp_doc */
   0,                                              /* tp_traverse */
   0,                                              /* tp_clear */
   0,                                              /* tp_richcompare */
   0,                                              /* tp_weaklistoffset */
   0,                                              /* tp_iter */
   0,                                              /* tp_iternext */
   Connection_methods,                             /* tp_methods */
   0,                                              /* tp_members */
   0,                                              /* tp_getset */
   0,                                              /* tp_base */
   0,                                              /* tp_dict */
   0,                                              /* tp_descr_get */
   0,                                              /* tp_descr_set */
   0,                                              /* tp_dictoffset */
   (initproc)Connection_init,                      /* tp_init */
   0,                                              /* tp_alloc */
   Connection_new,                                 /* tp_new */
};

static PyMethodDef module_methods[] = {
   {NULL}  /* Sentinel */
};

#ifndef PyMODINIT_FUNC/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initmysqldb(void)
{
   PyObject* module;

   /*
    * Initialize new object types 
    */
   if (PyType_Ready(&Connection_Type) < 0) return;
   if (PyType_Ready(&Result_Type) < 0) return;

   /*
    * Create a Python module
    */
   module = Py_InitModule3("mysqldb", module_methods, "MySQL wrapper module");
   if (module == NULL) return;

   /*
    * Add Connection class into module's namespace.
    */
   Py_INCREF(&Connection_Type);
   PyModule_AddObject(module, "Connection", (PyObject *)&Connection_Type);
   
//   Py_INCREF(&Result_Type);
//   PyModule_AddObject(m, "Result", (PyObject *)&Result_Type);
}

