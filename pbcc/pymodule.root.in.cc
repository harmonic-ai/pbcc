// This file is compiled once. This defines the Python module's entry point.

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <Python.h>

#include "pymodule.support.hh"
// __COMPILER__FOREACH_MODULE__
#include "__COMPILER__QUALIFIED_OUTPUT_MODULE_NAME__.impl.__COMPILER__MODULE_NAME__.hh"
// __COMPILER__END_FOREACH__

static PyMethodDef module_methods[] = {
    // __COMPILER__FOREACH_MODULE__
    // __COMPILER__FOREACH_MESSAGE__
    {"__construct____COMPILER__MESSAGE_CC_NAME__", +[](PyObject*, PyObject*) -> PyObject* {
       return PyObject_CallNoArgs(reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
     },
        METH_NOARGS, ""},
    // __COMPILER__END_FOREACH__
    // __COMPILER__END_FOREACH__
    {nullptr, nullptr, 0, nullptr},
};

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    "__COMPILER__QUALIFIED_OUTPUT_MODULE_NAME__", // m_name
    nullptr, // m_doc
    -1, // m_size
    module_methods, // m_methods
    nullptr, // m_reload
    nullptr, // m_traverse
    nullptr, // m_clear
    nullptr, // m_free
};

// __COMPILER__FOREACH_MODULE__
struct PyModuleDef __COMPILER__MODULE_NAME___module_def = {
    PyModuleDef_HEAD_INIT,
    "__COMPILER__MODULE_NAME__", // m_name
    nullptr, // m_doc
    -1, // m_size
    nullptr, // m_methods
    nullptr, // m_reload
    nullptr, // m_traverse
    nullptr, // m_clear
    nullptr, // m_free
};
// __COMPILER__END_FOREACH__

extern "C" PyMODINIT_FUNC PyInit___COMPILER__BASE_OUTPUT_MODULE_NAME__(void) {
  return handle_python_errors([&]() -> PyObject* {
    PyObjectRef<> m = raise_python_errors(PyModule_Create2, &module_def, PYTHON_API_VERSION);

    // Ready all the message types and create the enum classes
    // __COMPILER__FOREACH_MODULE__
    // __COMPILER__FOREACH_MESSAGE__
    if (PyType_Ready(&__COMPILER__MESSAGE_CC_NAME__::py_type) < 0) {
      throw python_error("");
    }
    // __COMPILER__END_FOREACH__
    // __COMPILER__FOREACH_ENUM__
    __COMPILER__ENUM_CC_NAME___enum_ref.create_py_enum("__COMPILER__QUALIFIED_OUTPUT_MODULE_NAME__");
    // __COMPILER__END_FOREACH__
    // __COMPILER__END_FOREACH__

    // Add all the submodules to the main module
    // __COMPILER__FOREACH_MODULE__
    {
      PyObjectRef<> sub_m = raise_python_errors(PyModule_Create2, &__COMPILER__MODULE_NAME___module_def, PYTHON_API_VERSION);
      if (PyModule_AddObjectRef(m.borrow(), "__COMPILER__MODULE_NAME__", sub_m.borrow())) {
        throw python_error("");
      }
    }
    // __COMPILER__END_FOREACH__

    // Add all the message and enum classes to the module

    // Per-module messages and enums
    // __COMPILER__FOREACH_MODULE__
    // __COMPILER__FOREACH_MESSAGE__
    add_object_to_module(m.borrow(), "__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__", reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
    {
      __COMPILER__MESSAGE_CC_NAME__::py_free_constructor = PyObject_GetAttrString(m.borrow(), "__construct____COMPILER__MESSAGE_CC_NAME__");
      if (!__COMPILER__MESSAGE_CC_NAME__::py_free_constructor) {
        throw python_error("");
      }
      Py_INCREF(__COMPILER__MESSAGE_CC_NAME__::py_free_constructor);
    }
    // __COMPILER__END_FOREACH__
    // __COMPILER__FOREACH_ENUM__
    add_object_to_module(m.borrow(), "__COMPILER__MODULE_NAME__.__COMPILER__ENUM_PYTHON_NAME_ESCAPED__", __COMPILER__ENUM_CC_NAME___enum_ref.py_enum_class().borrow());
    // __COMPILER__END_FOREACH__
    // __COMPILER__END_FOREACH__

    // Global aliases
    // __COMPILER__FOREACH_GLOBAL_MESSAGE_ALIAS__
    add_object_to_module(m.borrow(), "__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__", reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
    // __COMPILER__END_FOREACH__
    // __COMPILER__FOREACH_GLOBAL_ENUM_ALIAS__
    add_object_to_module(m.borrow(), "__COMPILER__ENUM_PYTHON_NAME_ESCAPED__", __COMPILER__ENUM_CC_NAME___enum_ref.py_enum_class().borrow());
    // __COMPILER__END_FOREACH__

    // Release the module pointer. If anything above raises, the reference
    // won't be released here (and returned) and will instead be destroyed by
    // the PyObjectRef destructor, so memory won't be leaked in case of
    // exceptions
    return m.release();
  });
}
