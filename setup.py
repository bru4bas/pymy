
from distutils.core import setup, Extension
from subprocess import check_output

compile_args = check_output(['mysql_config', '--cflags'])
link_args = check_output(['mysql_config', '--libs'])

sources = [ 'src/pymy.c' ]

ext = Extension( 'pymy',
                 sources,
                 extra_compile_args = compile_args.split(),
                 extra_link_args = link_args.split())

setup(
    name="pymy", version="0.2",
    ext_modules=[ ext ])


