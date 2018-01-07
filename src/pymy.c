
#include <mysql.h>
#include <Python.h>
#include <stdlib.h>
#include "structmember.h"
#include "datetime.h"

typedef struct {
   PyObject_HEAD
   MYSQL *con;                               // MySQL connection handler
} Database_Object;

typedef struct {
   PyObject_HEAD
   PyObject *fields;                         // Tuple with field names (Python strings)
   int *types;                               // Types of MySQL individual fields
   MYSQL_RES *res;                           // MySQL stored result 
   MYSQL_ROW last;                           // last read row from result
   unsigned row;                             // last row index
   unsigned num_rows;                        // total number of rows in result
} Table_Object;

void 
Table_dealloc(PyObject *self)
{
   Table_Object *s = (Table_Object*)self;
   mysql_free_result(s->res);
   Py_XDECREF(s->fields);
   free(s->types);
   Py_TYPE(self)->tp_free(self);
}

PyObject *
convert_mysql_value(char *cvalue, unsigned type)
{
   PyObject *res;
   if(cvalue == NULL) {
      /*
       * NULL values return None type
       */
      Py_INCREF(Py_None);
      return Py_None;
   }

   switch(type) {
      /*
       * Integer types.
       */
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONGLONG:
         res = PyInt_FromString(cvalue, NULL, 10);
         break;

      /*
       * Floating point types.
       */
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
         res = PyFloat_FromDouble(atof(cvalue));
         break;

      /*
       * Date types.
       */
      case MYSQL_TYPE_DATE:
         res = PyDate_FromDate(atoi(cvalue), atoi(cvalue+5), atoi(cvalue+8));
         break;

      case MYSQL_TYPE_TIME:
         res = PyTime_FromTime(atoi(cvalue), atoi(cvalue+3), atoi(cvalue+6), 0);
         break;

      case MYSQL_TYPE_DATETIME:
         res = PyDateTime_FromDateAndTime(atoi(cvalue), atoi(cvalue+5), atoi(cvalue+8),
                                          atoi(cvalue+11), atoi(cvalue+14), atoi(cvalue+17), 0);
         break;

      case MYSQL_TYPE_TIMESTAMP:
         res = PyDateTime_FromDateAndTime(atoi(cvalue), atoi(cvalue+5), atoi(cvalue+8),
                                          atoi(cvalue+11), atoi(cvalue+14), atoi(cvalue+17), atoi(cvalue+20));
         break;

      /*
       * All the rest are kept as strings
       */
      default:
         res = PyString_FromString(cvalue);
         break;
   }

   return res;
}

PyObject *
Table_getitem(PyObject *self, Py_ssize_t index)
{
   Table_Object *s = (Table_Object*)self;
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
      PyObject *value = convert_mysql_value(s->last[i], s->types[i]);
      if(value == NULL) {
         Py_DECREF(item);
         return NULL;
      }
      if(PyDict_SetItem(item, key, value) < 0) {
         Py_DECREF(item);
         return NULL;
      }
   }

   return item;   
}

PyObject *
Table_getcolumn(PyObject *self, PyObject *args)
{
   Table_Object *s = (Table_Object*)self;
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
      PyTuple_SetItem(col, j, convert_mysql_value(s->last[i], s->types[i]));
   }

   return col;
}

Py_ssize_t 
Table_getsize(PyObject *self)
{
   Table_Object *s = (Table_Object*)self;
   return s->num_rows;
}

/*
 * Table can work as a (imutable) sequence.
 */
static PySequenceMethods Table_as_sequence = {
   Table_getsize,                                  /* sq_length */
   0,                                              /* sq_concat */
   0,                                              /* sq_repeat */
   Table_getitem,                                  /* sq_item */
   0,                                              /* sq_ass_item */
   0,                                              /* sq_contains */
   0,                                              /* sq_inplace_concat */
   0                                               /* sq_inplace_repeat */
};

/*
 * Members for Table Object
 */
static PyMemberDef Table_members[] = {
   { "fields", T_OBJECT, offsetof(Table_Object, fields), READONLY, "Field names" },
   { NULL }
};

/*
 * C Methods for Table Object
 */
static PyMethodDef Table_methods[] = {
   { "column", (PyCFunction)Table_getcolumn, METH_VARARGS, "Tuple with all values of the column" },
   { NULL }
};

/*
 * Table Type definition for Python
 */
static PyTypeObject Table_Type = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "pymy.Table",                                   /* tp_name */
   sizeof(Table_Object),                           /* tp_basicsize */
   0,                                              /* tp_itemsize */
   (destructor)Table_dealloc,                      /* tp_dealloc */
   0,                                              /* tp_print */
   0,                                              /* tp_getattr */
   0,                                              /* tp_setattr */
   0,                                              /* tp_compare */
   0,                                              /* tp_repr */
   0,                                              /* tp_as_number */
   &Table_as_sequence,                             /* tp_as_sequence */
   0,                                              /* tp_as_mapping */
   0,                                              /* tp_hash */
   0,                                              /* tp_call */
   0,                                              /* tp_str */
   0,                                              /* tp_getattro */
   0,                                              /* tp_setattro */
   0,                                              /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,       /* tp_flags */
   "A Table with MySQL Query Results",             /* tp_doc */
   0,                                              /* tp_traverse */
   0,                                              /* tp_clear */
   0,                                              /* tp_richcompare */
   0,                                              /* tp_weaklistoffset */
   0,                                              /* tp_iter */
   0,                                              /* tp_iternext */
   Table_methods,                                  /* tp_methods */
   Table_members,                                  /* tp_members */
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
Database_new(PyTypeObject *type, PyObject *args, PyObject *keywords)
{
   Database_Object *self = (Database_Object*)type->tp_alloc(type, 0);
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
Database_dealloc(PyObject *self)
{
   Database_Object *s = (Database_Object*)self;
   if(s->con != NULL)
      mysql_close(s->con);
   Py_TYPE(self)->tp_free(self);
}

static int
Database_init(Database_Object *self, PyObject *args, PyObject *keywords)
{
   MYSQL *res;
   static char *keys[] = { "database", "host", "user", "password", NULL };
   char *db;
   char *host = "localhost";
   char *user = "root";
   char *pwd = "";
   
   if(!PyArg_ParseTupleAndKeywords(args, keywords, "s|sss", keys, &db, &host, &user, &pwd))
      return -1;
   
   Py_BEGIN_ALLOW_THREADS
   res = mysql_real_connect(self->con, host, user, pwd, db, 0, NULL, 0);
   Py_END_ALLOW_THREADS

   if(res == NULL) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(self->con));
      return -1;
   }

   return 0;
}

static PyObject *
Database_query(PyObject *self, PyObject *args)
{
   char *query;
   int nok;
   MYSQL_RES *res;
   Database_Object *s = (Database_Object*)self;

   /*
    * Send query to MySQL server
    */
   if(!PyArg_ParseTuple(args, "s", &query)) return NULL;
   Py_BEGIN_ALLOW_THREADS
   nok = mysql_query(s->con, query);
   Py_END_ALLOW_THREADS

   if(nok) {
      PyErr_SetString(PyExc_RuntimeError, mysql_error(s->con));
      return NULL;
   }

   /*
    * Read back results (store to memory).
    */
   Py_BEGIN_ALLOW_THREADS
   res = mysql_store_result(s->con);
   Py_END_ALLOW_THREADS

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
    * Create new Table object.
    */
   Table_Object *result = PyObject_New(Table_Object, &Table_Type);
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
   result->types = malloc(fields * sizeof(int));
   unsigned i;
   for(i=0; ;i++) {
      MYSQL_FIELD *f = mysql_fetch_field(res);
      if(f == NULL) break;
      PyTuple_SetItem(result->fields, i, PyString_FromString(f->name));
      result->types[i] = (int)f->type;
   }

   return (PyObject*)result;
}

static PyObject *
Database_exec(PyObject *self, PyObject *args)
{
   char *query;
   int nok;
   Database_Object *s = (Database_Object*)self;

   /*
    * Send query to MySQL server
    */
   if(!PyArg_ParseTuple(args, "s", &query)) return NULL;
   Py_BEGIN_ALLOW_THREADS
   nok = mysql_query(s->con, query);
   Py_END_ALLOW_THREADS
   if(nok) {
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
static PyMethodDef Database_methods[] = {
   { "query", (PyCFunction)Database_query, METH_VARARGS, "Execute a SQL query and return a result (None) if no result" },
   { "execute", (PyCFunction)Database_exec, METH_VARARGS, "Execute a SQL command and return the number of affected rows" },
   { NULL }
};

/*
 * Connection Type definition for Python
 */
static PyTypeObject Database_Type = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "pymy.Database",                                /* tp_name */
   sizeof(Database_Object),                        /* tp_basicsize */
   0,                                              /* tp_itemsize */
   (destructor)Database_dealloc,                   /* tp_dealloc */
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
   "MySQL Database Connection abstraction",        /* tp_doc */
   0,                                              /* tp_traverse */
   0,                                              /* tp_clear */
   0,                                              /* tp_richcompare */
   0,                                              /* tp_weaklistoffset */
   0,                                              /* tp_iter */
   0,                                              /* tp_iternext */
   Database_methods,                               /* tp_methods */
   0,                                              /* tp_members */
   0,                                              /* tp_getset */
   0,                                              /* tp_base */
   0,                                              /* tp_dict */
   0,                                              /* tp_descr_get */
   0,                                              /* tp_descr_set */
   0,                                              /* tp_dictoffset */
   (initproc)Database_init,                        /* tp_init */
   0,                                              /* tp_alloc */
   Database_new,                                   /* tp_new */
};

static PyMethodDef module_methods[] = {
   {NULL}  /* Sentinel */
};

#ifndef PyMODINIT_FUNC/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initpymy(void)
{
   PyObject* module;

   /*
    * Initialize new object types 
    */
   if (PyType_Ready(&Database_Type) < 0) return;
   if (PyType_Ready(&Table_Type) < 0) return;

   /*
    * Create a Python module
    */
   module = Py_InitModule3("pymy", module_methods, "MySQL wrapper module");
   if (module == NULL) return;

   /*
    * Add Connection class into module's namespace.
    */
   Py_INCREF(&Database_Type);
   Py_INCREF(&Table_Type);
   PyModule_AddObject(module, "Database", (PyObject *)&Database_Type);

   PyDateTime_IMPORT ;
}

