
from distutils.core import setup, Extension
from subprocess import check_output

compile_args = check_output(['mysql_config', '--cflags'])
link_args = check_output(['mysql_config', '--libs'])

sources = [ 'src/mysqldb.c' ]

ext = Extension( 'mysqldb',
                 sources,
                 extra_compile_args = compile_args.split(),
                 extra_link_args = link_args.split())

setup(
    name="mysqldb", version="1.0",
    ext_modules=[ ext ])


