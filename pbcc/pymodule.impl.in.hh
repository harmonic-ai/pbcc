// This file is compiled multiple times, resulting in a separate header for each proto module.

#pragma once

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

// __COMPILER__FOREACH_ENUM__
class __COMPILER__ENUM_CC_NAME__EnumRef : public PyEnumRef {
protected:
  virtual const char* get_python_name() const;
  virtual void populate_values(PyObject* dict);
};

extern __COMPILER__ENUM_CC_NAME__EnumRef __COMPILER__ENUM_CC_NAME___enum_ref;
// __COMPILER__END_FOREACH__

// __COMPILER__FOREACH_MESSAGE__
struct __COMPILER__MESSAGE_CC_NAME__ {
  // clang-format off
  PyObject_HEAD

  struct MessageData {
    // clang-format on

    // Fields visible to Python code
    // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
    // __COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__
    // Field number __COMPILER__MESSAGE_FIELD_NUMBER__
    // __COMPILER__END_FOREACH__
    PyObjectRef<> py___COMPILER__MESSAGE_FIELD_GROUP_NAME__;
    // __COMPILER__END_FOREACH__
    std::unordered_multimap<uint64_t, std::string> unknown_fields; // {tag: data}
  };

  MessageData data;

  // All methods prefixed with py_ are to be called by Python callers; all other methods are to be called from C++ only

  // Base class constructor/destructor
  static __COMPILER__MESSAGE_CC_NAME__* new_with_default_values(PyTypeObject* type);
  static PyObject* py_new(PyTypeObject* type, PyObject* args, PyObject* kwargs);
  static int py_init(PyObject* self, PyObject* args, PyObject* kwargs);
  static void py_dealloc(PyObject* py_self);

  // Lifecycle methods
  static PyObject* py_proto_copy(PyObject* self, PyObject* args, PyObject* kwargs);

  // Protobuf parsing/serializing functions
  void parse_unknown_field(StringReader& r, uint64_t tag, uint8_t flags);
  void handle_incorrect_type(StringReader& r, uint64_t tag, DataType expected_type, uint8_t flags);
  void parse_proto_into_this(const void* data, size_t size, uint8_t flags);
  static __COMPILER__MESSAGE_CC_NAME__* from_proto_data(const void* data, size_t size, uint8_t flags);
  static PyObject* py_parse_proto_into_this(PyObject* self, PyObject* args, PyObject* kwargs);
  static PyObject* py_from_proto_data(PyObject* self, PyObject* args, PyObject* kwargs);
  static void as_proto_data(PyObject* py_self, StringWriter& w);
  static PyObject* py_as_proto_data(PyObject* py_self);

  // Pickle support
  static PyObject* py_reduce(PyObject* self);
  static PyObject* py_setstate(PyObject* self, PyObject* state);

  // Utility functions
  static PyObject* py_as_dict(PyObject* self);
  static PyObject* py_has_unknown_fields(PyObject* py_self);
  static PyObject* py_delete_unknown_fields(PyObject* py_self);
  static PyObject* py_repr(PyObject* py_self);
  static PyObject* py_richcompare(PyObject* py_self, PyObject* py_other, int op); // Implements equality operators

  static PyMemberDef py_members[];
  static PyMethodDef py_methods[];
  static PyTypeObject py_type;
  static PyObject* py_free_constructor;
};
// __COMPILER__END_FOREACH__

extern struct PyModuleDef __COMPILER__MODULE_NAME___module_def;
