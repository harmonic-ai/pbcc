// This file is compiled multiple times, resulting in a separate TU for each proto module.

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

// Include all other modules' headers, so we can support cross-module imports
// __COMPILER__FOREACH_MODULE__
#include "__COMPILER__QUALIFIED_OUTPUT_MODULE_NAME__.impl.__COMPILER__MODULE_NAME__.hh"
// __COMPILER__END_FOREACH__
#include "pymodule.support.hh"

// __COMPILER__FOREACH_ENUM__
const char* __COMPILER__ENUM_CC_NAME__EnumRef::get_python_name() const {
  return "__COMPILER__MODULE_NAME__.__COMPILER__ENUM_PYTHON_NAME__";
}

void __COMPILER__ENUM_CC_NAME__EnumRef::populate_values(PyObject* dict) {
  // __COMPILER__FOREACH_ENUM_MEMBER__
  {
    PyObjectRef<> __COMPILER__ENUM_MEMBER_NAME___value = raise_python_errors(PyLong_FromLong, __COMPILER__ENUM_MEMBER_VALUE__);
    if (PyDict_SetItemString(dict, "__COMPILER__ENUM_MEMBER_NAME__", __COMPILER__ENUM_MEMBER_NAME___value.borrow())) {
      throw python_error("");
    }
  }
  // __COMPILER__END_FOREACH__
}

__COMPILER__ENUM_CC_NAME__EnumRef __COMPILER__ENUM_CC_NAME___enum_ref;
// __COMPILER__END_FOREACH__

// __COMPILER__FOREACH_MESSAGE__
PyObject* __COMPILER__MESSAGE_CC_NAME__::py_free_constructor = nullptr;

__COMPILER__MESSAGE_CC_NAME__* __COMPILER__MESSAGE_CC_NAME__::new_with_default_values(PyTypeObject* type) {

  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(type->tp_alloc(type, 0));
  if (!self) {
    throw python_error("");
  }
  new (&self->data) __COMPILER__MESSAGE_CC_NAME__::MessageData();

  // Populate defaults for all fields
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(
      __COMPILER__MESSAGE_FIELD_GROUP_DEFAULT_VALUE_CONSTRUCTOR__);
  // __COMPILER__END_FOREACH__
  return self;
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_new(PyTypeObject* type, PyObject*, PyObject*) {
  return handle_python_errors(__COMPILER__MESSAGE_CC_NAME__::new_with_default_values, type);
}

int __COMPILER__MESSAGE_CC_NAME__::py_init(PyObject* py_self, PyObject* args, PyObject* kwargs) {
  __COMPILER__MESSAGE_CC_NAME__* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  static const char* kwarg_names[] = {
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      "__COMPILER__MESSAGE_FIELD_GROUP_NAME__",
      // __COMPILER__END_FOREACH__
      nullptr,
  };
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  PyObject* arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__ = nullptr;
  // __COMPILER__END_FOREACH__
  // clang-format off
  int parse_ret = PyArg_ParseTupleAndKeywords(args, kwargs, "|"
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      "O"
      // __COMPILER__END_FOREACH__
      , const_cast<char**>(kwarg_names)
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      , &arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__
      // __COMPILER__END_FOREACH__
  );
  // clang-format on
  if (!parse_ret) {
    return -1;
  }

  // Populate values for all fields that were specified
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  if (arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__) {
    Py_INCREF(arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__);
    self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__);
  }
  // __COMPILER__END_FOREACH__

  return 0;
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_proto_copy(PyObject* py_self, PyObject* args, PyObject* kwargs) {
  __COMPILER__MESSAGE_CC_NAME__* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  static const char* kwarg_names[] = {
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      "__COMPILER__MESSAGE_FIELD_GROUP_NAME__",
      // __COMPILER__END_FOREACH__
      nullptr,
  };
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  PyObject* arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__ = nullptr;
  // __COMPILER__END_FOREACH__
  // clang-format off
  int parse_ret = PyArg_ParseTupleAndKeywords(args, kwargs, "|"
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      "O"
      // __COMPILER__END_FOREACH__
      , const_cast<char**>(kwarg_names)
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      , &arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__
      // __COMPILER__END_FOREACH__
  );
  // clang-format on
  if (!parse_ret) {
    return nullptr;
  }

  // Make a new one with default values
  PyObjectRef<__COMPILER__MESSAGE_CC_NAME__> new_obj = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(
      py_type.tp_alloc(&py_type, 0));
  if (!new_obj) {
    throw python_error("");
  }
  new (&new_obj->data) __COMPILER__MESSAGE_CC_NAME__::MessageData();

  // Populate values for all fields that were specified, falling back to self
  // for values not specified
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  if (arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__) {
    Py_INCREF(arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__);
    new_obj->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(arg___COMPILER__MESSAGE_FIELD_GROUP_NAME__);
  } else {
    new_obj->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(
        self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.new_ref());
  }
  // __COMPILER__END_FOREACH__

  return reinterpret_cast<PyObject*>(new_obj.release());
}

void __COMPILER__MESSAGE_CC_NAME__::py_dealloc(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  // Delete all held Python object references and clear unknown_fields
  self->data.~MessageData();
  Py_TYPE(self)->tp_free(self);
}

void __COMPILER__MESSAGE_CC_NAME__::parse_unknown_field(StringReader& r, uint64_t tag, uint8_t flags) {
  if (flags & ParseFlag::RETAIN_UNKNOWN_FIELDS) {
    size_t start_offset = r.where();
    skip_field(r, wire_type_for_tag(tag));
    this->data.unknown_fields.emplace(tag, r.preadx(start_offset, r.where() - start_offset));
  } else {
    skip_field(r, wire_type_for_tag(tag));
  }
}

void __COMPILER__MESSAGE_CC_NAME__::handle_incorrect_type(
    StringReader& r, uint64_t tag, DataType expected_type, uint8_t flags) {
  if (!(flags & ParseFlag::IGNORE_INCORRECT_TYPES)) {
    throw_incorrect_type(wire_type_for_data_type(expected_type), wire_type_for_tag(tag));
  } else {
    this->parse_unknown_field(r, tag, flags);
  }
}

void __COMPILER__MESSAGE_CC_NAME__::parse_proto_into_this(const void* data, size_t size, uint8_t flags) {
  StringReader r(data, size);
  while (!r.eof()) {
    uint64_t tag = decode_varint(r);
    WireType received_type = wire_type_for_tag(tag);
    switch (field_num_for_tag(tag)) {
      // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
      // __COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__
      case __COMPILER__MESSAGE_FIELD_NUMBER__:
        try {
          // __COMPILER__IF_MESSAGE_FIELD_TYPE_NOT_REPEATED__
          if (received_type == wire_type_for_data_type(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__)) {
            this->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(
                TypeCodec<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>::parse(
                    r,
                    __COMPILER__MESSAGE_FIELD_ENUM_REF__,
                    __COMPILER__MESSAGE_FIELD_MESSAGE_PARSE_FN__,
                    flags));
          } else {
            this->handle_incorrect_type(r, tag, DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__, flags);
          }
          // __COMPILER__END_IF__
          // __COMPILER__IF_MESSAGE_FIELD_TYPE_REPEATED__
          if (can_use_packed_repeated_format(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__) &&
              (received_type == WireType::LENGTH)) {
            parse_packed_repeated<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>(
                this->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
                r,
                __COMPILER__MESSAGE_FIELD_ENUM_REF__,
                __COMPILER__MESSAGE_FIELD_MESSAGE_PARSE_FN__,
                flags);
          } else if (received_type == wire_type_for_data_type(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__)) {
            parse_unpacked_repeated<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>(
                this->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
                r,
                __COMPILER__MESSAGE_FIELD_ENUM_REF__,
                __COMPILER__MESSAGE_FIELD_MESSAGE_PARSE_FN__,
                flags);
          } else {
            this->handle_incorrect_type(r, tag, DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__, flags);
          }
          // __COMPILER__END_IF__
          // __COMPILER__IF_MESSAGE_FIELD_TYPE_MAP__
          static_assert(wire_type_for_data_type(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__) == WireType::LENGTH,
              "Map-valued field does not expect MESSAGE data type");
          if (received_type == WireType::LENGTH) {
            parse_map<DataType::__COMPILER__MESSAGE_FIELD_KEY_TYPE__, DataType::__COMPILER__MESSAGE_FIELD_VALUE_TYPE__>(
                this->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
                r,
                __COMPILER__MESSAGE_FIELD_VALUE_ENUM_REF__,
                __COMPILER__MESSAGE_FIELD_VALUE_MESSAGE_PARSE_FN__,
                flags);
          } else {
            this->handle_incorrect_type(r, tag, DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__, flags);
          }
          // __COMPILER__END_IF__
        } catch (const python_error& e) {
          auto prefix = string_printf(
              "(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__#__COMPILER__MESSAGE_FIELD_NUMBER__+0x%zX) ", r.where());
          throw python_error(prefix + e.what());
        } catch (const std::exception& e) {
          auto prefix = string_printf(
              "(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__#__COMPILER__MESSAGE_FIELD_NUMBER__+0x%zX) ", r.where());
          throw std::runtime_error(prefix + e.what());
        }
        break;
        // __COMPILER__END_FOREACH__
        // __COMPILER__END_FOREACH__
      default:
        try {
          this->parse_unknown_field(r, tag, flags);
        } catch (const python_error& e) {
          auto prefix = string_printf("(at 0x%zX) ", r.where());
          throw python_error(prefix + e.what());
        } catch (const std::exception& e) {
          auto prefix = string_printf("(at 0x%zX) ", r.where());
          throw std::runtime_error(prefix + e.what());
        }
    }
  }
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_parse_proto_into_this(PyObject* self, PyObject* args, PyObject* kwargs) {
  static const char* kwarg_names[] = {"data", "retain_unknown_fields", "ignore_incorrect_types", nullptr};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);

  const void* input_data;
  Py_ssize_t input_size;
  int retain_unknown_fields = 1;
  int ignore_incorrect_types = 0;
  if (!PyArg_ParseTupleAndKeywords(
          args,
          kwargs,
          "y#|pp",
          kwarg_names_arg,
          &input_data,
          &input_size,
          &retain_unknown_fields,
          &ignore_incorrect_types)) {
    return nullptr;
  }

  uint8_t flags = ((retain_unknown_fields ? ParseFlag::RETAIN_UNKNOWN_FIELDS : 0) |
      (ignore_incorrect_types ? ParseFlag::IGNORE_INCORRECT_TYPES : 0));

  return handle_python_errors([&]() -> PyObject* {
    reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(self)->parse_proto_into_this(input_data, input_size, flags);
    Py_INCREF(Py_None);
    return Py_None;
  });
}

__COMPILER__MESSAGE_CC_NAME__* __COMPILER__MESSAGE_CC_NAME__::from_proto_data(
    const void* data, size_t size, uint8_t flags) {
  PyObjectRef<__COMPILER__MESSAGE_CC_NAME__> self = __COMPILER__MESSAGE_CC_NAME__::new_with_default_values(
      &__COMPILER__MESSAGE_CC_NAME__::py_type);
  self->parse_proto_into_this(data, size, flags);
  return self.release();
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_from_proto_data(PyObject*, PyObject* args, PyObject* kwargs) {
  static const char* kwarg_names[] = {"data", "retain_unknown_fields", "ignore_incorrect_types", nullptr};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);

  const void* input_data;
  Py_ssize_t input_size;
  int retain_unknown_fields = 1;
  int ignore_incorrect_types = 0;
  if (!PyArg_ParseTupleAndKeywords(
          args,
          kwargs,
          "y#|pp",
          kwarg_names_arg,
          &input_data,
          &input_size,
          &retain_unknown_fields,
          &ignore_incorrect_types)) {
    return nullptr;
  }

  uint8_t flags = ((retain_unknown_fields ? ParseFlag::RETAIN_UNKNOWN_FIELDS : 0) |
      (ignore_incorrect_types ? ParseFlag::IGNORE_INCORRECT_TYPES : 0));

  return handle_python_errors(__COMPILER__MESSAGE_CC_NAME__::from_proto_data, input_data, input_size, flags);
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_reduce(PyObject* py_self) {
  // We have to use a free function as the constructor, since the pickle module doesn't know what to do with our
  // submodule structure. We instead tell it to call the free function, which directly delegates to the constructor.
  return Py_BuildValue("O()N",
      __COMPILER__MESSAGE_CC_NAME__::py_free_constructor,
      __COMPILER__MESSAGE_CC_NAME__::py_as_proto_data(py_self));
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_setstate(PyObject* py_self, PyObject* state) {
  if (!PyBytes_Check(state)) {
    PyErr_SetString(PyExc_TypeError, "State must be a bytes object");
    return nullptr;
  }

  char* data;
  ssize_t size;
  if (PyBytes_AsStringAndSize(state, &data, &size)) {
    return nullptr;
  }

  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  return handle_python_errors([&]() -> PyObject* {
    self->parse_proto_into_this(data, size, false);
    Py_INCREF(Py_None);
    return Py_None;
  });
}

void __COMPILER__MESSAGE_CC_NAME__::as_proto_data(PyObject* py_self, StringWriter& w) {
  int is_this_type = PyObject_IsInstance(py_self, reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
  if (is_this_type == 1) {
    __COMPILER__MESSAGE_CC_NAME__* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);

    // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
    try {
      // __COMPILER__IF_MESSAGE_FIELD_GROUP_IS_ONEOF__
      static const SerializeOneofParams __COMPILER__MESSAGE_FIELD_GROUP_NAME___serialize_oneof_params[] = {
          // __COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__
          SerializeOneofParams{
              .field_num = __COMPILER__MESSAGE_FIELD_NUMBER__,
              .is_optional = __COMPILER__MESSAGE_FIELD_IS_OPTIONAL__,
              .enum_ref = __COMPILER__MESSAGE_FIELD_ENUM_REF__,
              .serialize_message = __COMPILER__MESSAGE_FIELD_MESSAGE_SERIALIZE_FN__,
              .message_type_obj = __COMPILER__MESSAGE_FIELD_SUBMESSAGE_TYPE_OBJ__,
          },
          // __COMPILER__END_FOREACH__
      };
      serialize_oneof_with_tag<
          // __COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__
          DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__,
          // __COMPILER__END_FOREACH__
          DataType::UNKNOWN>(
          w,
          self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
          __COMPILER__MESSAGE_FIELD_GROUP_NAME___serialize_oneof_params);
      // __COMPILER__END_IF__
      // __COMPILER__IF_MESSAGE_FIELD_GROUP_IS_NOT_ONEOF__
      // __COMPILER__FOREACH_MESSAGE_FIELD_IN_GROUP__
      // __COMPILER__IF_MESSAGE_FIELD_TYPE_NOT_REPEATED__
      if (!TypeCodec<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>::value_matches_type(
              self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
              __COMPILER__MESSAGE_FIELD_ENUM_REF__,
              __COMPILER__MESSAGE_FIELD_SUBMESSAGE_TYPE_OBJ__,
              __COMPILER__MESSAGE_FIELD_IS_OPTIONAL__)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow()));
      }
      serialize_with_tag<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>(
          w,
          __COMPILER__MESSAGE_FIELD_NUMBER__,
          __COMPILER__MESSAGE_FIELD_IS_OPTIONAL__ ? DefaultBehavior::OPTIONAL : DefaultBehavior::REQUIRED,
          self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
          __COMPILER__MESSAGE_FIELD_ENUM_REF__,
          __COMPILER__MESSAGE_FIELD_MESSAGE_SERIALIZE_FN__);
      // __COMPILER__END_IF__
      // __COMPILER__IF_MESSAGE_FIELD_TYPE_REPEATED__
      serialize_repeated_with_tag<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>(
          w,
          __COMPILER__MESSAGE_FIELD_NUMBER__,
          self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
          __COMPILER__MESSAGE_FIELD_ENUM_REF__,
          __COMPILER__MESSAGE_FIELD_MESSAGE_SERIALIZE_FN__,
          __COMPILER__MESSAGE_FIELD_SUBMESSAGE_TYPE_OBJ__);
      // __COMPILER__END_IF__
      // __COMPILER__IF_MESSAGE_FIELD_TYPE_MAP__
      serialize_map_with_tag<DataType::__COMPILER__MESSAGE_FIELD_KEY_TYPE__, DataType::__COMPILER__MESSAGE_FIELD_VALUE_TYPE__>(
          w,
          __COMPILER__MESSAGE_FIELD_NUMBER__,
          self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
          __COMPILER__MESSAGE_FIELD_VALUE_ENUM_REF__,
          __COMPILER__MESSAGE_FIELD_VALUE_MESSAGE_SERIALIZE_FN__,
          __COMPILER__MESSAGE_FIELD_VALUE_SUBMESSAGE_TYPE_OBJ__);
      // __COMPILER__END_IF__
      // __COMPILER__END_FOREACH__
      // __COMPILER__END_IF__
    } catch (const python_error& e) {
      static const std::string prefix = "(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__) ";
      throw python_error(prefix + e.what());
    } catch (const std::exception& e) {
      static const std::string prefix = "(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__) ";
      throw std::runtime_error(prefix + e.what());
    }
    // __COMPILER__END_FOREACH__

    // Write unknown fields
    for (const auto& it : self->data.unknown_fields) {
      encode_varint(w, it.first);
      w.write(it.second);
    }

  } else if (is_this_type == 0) {
    throw std::invalid_argument("Field expected to be __COMPILER__MESSAGE_CC_NAME__ but it isn\'t");
  } else {
    throw python_error("");
  }
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_as_proto_data(PyObject* py_self) {
  return handle_python_errors([&]() -> PyObject* {
    StringWriter w;
    __COMPILER__MESSAGE_CC_NAME__::as_proto_data(py_self, w);
    return raise_python_errors(PyBytes_FromStringAndSize, w.str().data(), w.str().size());
  });
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_as_dict(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  return handle_python_errors([&]() -> PyObject* {
    PyObjectRef<> dict = raise_python_errors(PyDict_New);
    // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
    {
      PyObjectRef<> value = py_dict_value_for_value(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow());
      PyDict_SetItemString(dict.borrow(), "__COMPILER__MESSAGE_FIELD_GROUP_NAME__", value.borrow());
    }
    // __COMPILER__END_FOREACH__
    return dict.release();
  });
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_delete_unknown_fields(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  self->data.unknown_fields.clear();
  Py_INCREF(Py_None);
  return Py_None;
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_has_unknown_fields(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  if (self->data.unknown_fields.empty()) {
    Py_INCREF(Py_False);
    return Py_False;
  } else {
    Py_INCREF(Py_True);
    return Py_True;
  }
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_repr(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  return handle_python_errors([&]() -> PyObject* {
    PyObjectRef<> tokens = raise_python_errors(PyList_New, 0);
    // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
    {
      PyObjectRef<> value_repr;
      if (PyBytes_Check(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow())) {
        ssize_t size = PyBytes_Size(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow());
        if (size > REPR_STRING_MAX_BYTES) {
          value_repr.assign_ref(raise_python_errors(PyUnicode_FromFormat, "(%zd bytes)", size));
        } else {
          value_repr.assign_ref(raise_python_errors(PyObject_Repr, self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow()));
        }
      } else if (PyUnicode_Check(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow())) {
        ssize_t size = PyUnicode_GetLength(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow());
        if (size > REPR_STRING_MAX_CHARACTERS) {
          value_repr.assign_ref(raise_python_errors(PyUnicode_FromFormat, "(%zd chars)", size));
        } else {
          value_repr.assign_ref(raise_python_errors(PyObject_Repr, self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow()));
        }
      } else {
        value_repr.assign_ref(raise_python_errors(PyObject_Repr, self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow()));
      }
      PyObjectRef<> arg_str = raise_python_errors(PyUnicode_FromFormat, "__COMPILER__MESSAGE_FIELD_GROUP_NAME__=%S", value_repr.borrow());
      if (PyList_Append(tokens.borrow(), arg_str.borrow())) {
        throw python_error("");
      }
    }
    // __COMPILER__END_FOREACH__
    PyObjectRef<> separator = raise_python_errors(PyUnicode_FromString, ", ");
    PyObjectRef<> args_str = raise_python_errors(PyUnicode_Join, separator.borrow(), tokens.borrow());
    return raise_python_errors(PyUnicode_FromFormat, "__COMPILER__BASE_OUTPUT_MODULE_NAME__.__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME__(%S)", args_str.borrow());
  });
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_richcompare(PyObject* py_self, PyObject* py_other, int op) {
  if (op != Py_EQ && op != Py_NE) {
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
  }
  bool is_ne = (op == Py_NE);

  if (!PyObject_TypeCheck(py_other, &__COMPILER__MESSAGE_CC_NAME__::py_type)) {
    auto* ret = is_ne ? Py_True : Py_False;
    Py_INCREF(ret);
    return ret;
  }

  const auto* self = reinterpret_cast<const __COMPILER__MESSAGE_CC_NAME__*>(py_self);
  const auto* other = reinterpret_cast<const __COMPILER__MESSAGE_CC_NAME__*>(py_other);

  // Compare each field one by one, recursively
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  {
    PyObjectRef<> result = PyObject_RichCompare(
        self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
        other->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.borrow(),
        is_ne ? Py_NE : Py_EQ);
    if (result.borrow() == nullptr) {
      return nullptr;
    }
    // If we're doing an NE comparison and anything returns True, we're done (they are not equal). Similarly, if we're
    // doing EQ and anything returns False, we're done.
    if (result.borrow() != (is_ne ? Py_False : Py_True)) {
      return result.release();
    }
  }
  // __COMPILER__END_FOREACH__

  // We get here if every NE comparison above returned False or every EQ comparison returned True, so self and other
  // are actually equal. Return the appropriate boolean value.
  auto* ret = is_ne ? Py_False : Py_True;
  Py_INCREF(ret);
  return ret;
}

PyMemberDef __COMPILER__MESSAGE_CC_NAME__::py_members[] = {
    // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
    {"__COMPILER__MESSAGE_FIELD_GROUP_NAME__", T_OBJECT_EX, offsetof(__COMPILER__MESSAGE_CC_NAME__, data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__), 0, nullptr},
    // __COMPILER__END_FOREACH__
    {nullptr, 0, 0, 0, nullptr}, // End sentinel
};

PyMethodDef __COMPILER__MESSAGE_CC_NAME__::py_methods[] = {
    // Note: The double reinterpret_casts here essentially tell the compiler that we know what we're doing and it's OK
    // to lose the argument type information. See the notes on PyMethodDef::ml_meth in Python's docs:
    // https://docs.python.org/3/c-api/structures.html#c.PyMethodDef
    {
        "from_proto_data",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_from_proto_data)),
        METH_VARARGS | METH_KEYWORDS | METH_CLASS,
        "",
    },
    {
        "as_proto_data",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_as_proto_data)),
        METH_NOARGS,
        "",
    },
    {
        "proto_copy",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_proto_copy)),
        METH_VARARGS | METH_KEYWORDS,
        "",
    },
    {
        "as_dict",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_as_dict)),
        METH_NOARGS,
        "",
    },
    {
        "__reduce__",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_reduce)),
        METH_NOARGS,
        "",
    },
    {
        "__setstate__",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_setstate)),
        METH_O,
        "",
    },
    {
        "has_unknown_fields",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_has_unknown_fields)),
        METH_NOARGS,
        "",
    },
    {
        "delete_unknown_fields",
        reinterpret_cast<PyCFunction>(reinterpret_cast<void*>(&__COMPILER__MESSAGE_CC_NAME__::py_delete_unknown_fields)),
        METH_NOARGS,
        "",
    },
    {nullptr, nullptr, 0, nullptr}, // End sentinel
};

PyTypeObject __COMPILER__MESSAGE_CC_NAME__::py_type = {
    PyVarObject_HEAD_INIT(nullptr, 0) "__COMPILER__QUALIFIED_OUTPUT_MODULE_NAME__.__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME__", // tp_name
    sizeof(__COMPILER__MESSAGE_CC_NAME__), // tp_basicsize
    0, // tp_itemsize
    (destructor)__COMPILER__MESSAGE_CC_NAME__::py_dealloc, // tp_dealloc
    0, // tp_vectorcall_offset
    0, // tp_getattr
    0, // tp_setattr
    0, // tp_as_async
    __COMPILER__MESSAGE_CC_NAME__::py_repr, // tp_repr
    0, // tp_as_number
    0, // tp_as_sequence
    0, // tp_as_mapping
    0, // tp_hash
    0, // tp_call
    0, // tp_str
    0, // tp_getattro
    0, // tp_setattro
    0, // tp_as_buffer
    // TODO: Support cyclic garbage collection. See
    // https://docs.python.org/3/c-api/gcsupport.html#supporting-cycle-detection
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, // tp_flag
    0, // tp_doc
    0, // tp_traverse
    0, // tp_clear
    __COMPILER__MESSAGE_CC_NAME__::py_richcompare, // tp_richcompare
    0, // tp_weaklistoffset
    0, // tp_iter
    0, // tp_iternext
    __COMPILER__MESSAGE_CC_NAME__::py_methods, // tp_methods
    __COMPILER__MESSAGE_CC_NAME__::py_members, // tp_members
    0, // tp_getset
    0, // tp_base
    0, // tp_dict
    0, // tp_descr_get
    0, // tp_descr_set
    0, // tp_dictoffset
    __COMPILER__MESSAGE_CC_NAME__::py_init, // tp_init
    0, // tp_alloc
    __COMPILER__MESSAGE_CC_NAME__::py_new, // tp_new
    0, // tp_free
    0, // tp_is_gc
    0, // tp_bases
    0, // tp_mro
    0, // tp_cache
    0, // tp_subclasses
    0, // tp_weaklist
    0, // tp_del
    0, // tp_version_tag
    0, // tp_finalize
    0, // tp_vectorcall
};
// __COMPILER__END_FOREACH__
