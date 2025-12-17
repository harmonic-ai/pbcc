// This is the template for compiled protobuf parser/serializer modules.
// The compiler (compile.py) collects protobuf descriptors from a pb2.py
// module, then replaces all the compiler tags in this file with appropriate
// contents and compiles the result to a shared library.

// If a compiler tag appears within a line, it is replaced as-is and the rest
// of the line is preserved. Compiler tags that appear on comment lines by
// themselves denote blocks, which are used for for-each loops and conditions.

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

static constexpr ssize_t REPR_STRING_MAX_CHARACTERS = 10000;
static constexpr ssize_t REPR_STRING_MAX_BYTES = 100;

class python_error : public std::runtime_error {
public:
  using runtime_error::runtime_error;
};

template <typename Func, typename... ArgTs>
PyObject* raise_python_errors(Func&& func, ArgTs&&... args) {
  PyObject* ret = std::forward<Func>(func)(std::forward<ArgTs>(args)...);
  if (ret == nullptr) {
    throw python_error("");
  }
  return ret;
}

template <typename Func, typename... ArgTs>
PyObject* handle_python_errors(Func&& func, ArgTs&&... args) {
  try {
    return reinterpret_cast<PyObject*>(std::forward<Func>(func)(std::forward<ArgTs>(args)...));
  } catch (const python_error& e) {
    if (!PyErr_Occurred()) {
      throw std::logic_error("python_error exception caught without Python error state set");
    }

    PyObject *type, *value, *traceback;
    PyErr_Fetch(&type, &value, &traceback);

    if (value) {
      PyObject* prefixed_message = PyUnicode_FromFormat("%s%S", e.what(), value);
      PyErr_Restore(type, prefixed_message, traceback);
      Py_DECREF(value);
    } else {
      PyErr_Restore(type, value, traceback);
    }
    return nullptr;

  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  }
}

static constexpr bool is_in_u32_range(uint64_t v) {
  return (v & 0xFFFFFFFF00000000LL) == 0;
}
static constexpr bool is_in_s32_range(int64_t v) {
  return ((v >= -0x80000000LL) && (v <= 0x7FFFFFFFLL));
}

////////////////////////////////////////////////////////////////////////////////
// String reader/writer (from phosg)

__attribute__((format(printf, 1, 2))) std::string string_printf(const char* fmt, ...) {
  va_list va;
  va_start(va, fmt);
  char* result = nullptr;
  int length = vasprintf(&result, fmt, va);
  if (result == nullptr) {
    throw std::bad_alloc();
  }
  // NOTE: It's not great that we copy the string again here, but this is only
  // used in error cases so it's probably not a big deal
  std::string ret(result, length);
  free(result);
  va_end(va);
  return ret;
}

// Try to determine endianess from GCC defines first. If they aren't available,
// use some constants to try to figure it out
// clang-format off
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  // OK; system is little-endian
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  #error pbcc cannot be compiled on big-endian systems (for now)  
#else
  #define LITTLE_ENDIAN_VALUE 0x31323334UL
  #define BIG_ENDIAN_VALUE    0x34333231UL
  #define ENDIAN_ORDER_VALUE  ('1234')
  #if ENDIAN_ORDER_VALUE == LITTLE_ENDIAN_VALUE
  // OK; system is little-endian
  #elif ENDIAN_ORDER_VALUE == BIG_ENDIAN_VALUE
    #error pbcc cannot be compiled on big-endian systems (for now)
  #else
    #error "Unrecognized host system endianness"
  #endif
  #undef LITTLE_ENDIAN_VALUE
  #undef BIG_ENDIAN_VALUE
  #undef ENDIAN_ORDER_VALUE
#endif
// clang-format on

class StringReader {
public:
  StringReader()
      : data(nullptr),
        length(0),
        offset(0) {}
  StringReader(const void* data, size_t size, size_t offset = 0)
      : data(reinterpret_cast<const uint8_t*>(data)),
        length(size),
        offset(offset) {}
  virtual ~StringReader() = default;

  size_t where() const {
    return this->offset;
  }
  size_t size() const {
    return this->length;
  }
  size_t remaining() const {
    return this->length - this->offset;
  }
  inline void go(size_t offset) {
    this->offset = offset;
  }
  inline void skip(size_t bytes) {
    this->offset += bytes;
    if (this->offset > this->length) {
      this->offset = this->length;
      throw std::out_of_range("skip beyond end of string");
    }
  }
  inline bool eof() const {
    return (this->offset >= this->length);
  }

  StringReader subx(size_t offset) const {
    if (offset > this->length) {
      throw std::out_of_range("sub-reader begins beyond end of data");
    }
    return StringReader(
        reinterpret_cast<const char*>(this->data) + offset,
        this->length - offset);
  }
  StringReader subx(size_t offset, size_t size) const {
    if (offset + size > this->length) {
      throw std::out_of_range("sub-reader begins or extends beyond end of data");
    }
    return StringReader(reinterpret_cast<const char*>(this->data) + offset, size);
  }

  std::string preadx(size_t offset, size_t size) const {
    if (offset + size > this->length) {
      throw std::out_of_range("not enough data to read");
    }
    return std::string(reinterpret_cast<const char*>(this->data + offset), size);
  }

  inline const void* pgetv(size_t offset, size_t size) const {
    if (offset + size > this->length) {
      throw std::out_of_range("end of string");
    }
    return this->data + offset;
  }
#if defined(__x86_64__) || defined(_M_X64)
  template <typename T>
  const T& pget(size_t offset, size_t size = sizeof(T)) const {
    return *reinterpret_cast<const T*>(this->pgetv(offset, size));
  }
#else
  template <typename T>
  T pget(size_t offset, size_t size = sizeof(T)) const {
    T ret;
    memcpy(&ret, this->pgetv(offset, size), size);
    return ret;
  }
#endif

  inline const void* getv(size_t size, bool advance = true) {
    const void* ret = this->pgetv(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }

#if defined(__x86_64__) || defined(_M_X64)
  template <typename T>
  const T& get(bool advance = true, size_t size = sizeof(T)) {
    const T& ret = this->pget<T>(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }
#else
  template <typename T>
  T get(bool advance = true, size_t size = sizeof(T)) {
    T ret = this->pget<T>(this->offset, size);
    if (advance) {
      this->offset += size;
    }
    return ret;
  }
#endif

  // TODO: These should use the le_ types if we ever build this on big-endian systems
  inline uint8_t get_u8(bool advance = true) { return this->get<uint8_t>(advance); }
  inline int8_t get_s8(bool advance = true) { return this->get<int8_t>(advance); }
  inline uint16_t get_u16l(bool advance = true) { return this->get<uint16_t>(advance); }
  inline int16_t get_s16l(bool advance = true) { return this->get<int16_t>(advance); }
  inline uint32_t get_u32l(bool advance = true) { return this->get<uint32_t>(advance); }
  inline int32_t get_s32l(bool advance = true) { return this->get<int32_t>(advance); }
  inline uint64_t get_u64l(bool advance = true) { return this->get<uint64_t>(advance); }
  inline int64_t get_s64l(bool advance = true) { return this->get<int64_t>(advance); }
  inline float get_f32l(bool advance = true) { return this->get<float>(advance); }
  inline double get_f64l(bool advance = true) { return this->get<double>(advance); }

private:
  const uint8_t* data;
  size_t length;
  size_t offset;
};

class StringWriter {
public:
  StringWriter() = default;
  ~StringWriter() = default;

  inline size_t size() const {
    return this->data.size();
  }

  inline void write(const void* data, size_t size) {
    this->data.append(reinterpret_cast<const char*>(data), size);
  }
  inline void write(const std::string& data) {
    this->data.append(data);
  }

  template <typename T>
  void put(const T& v) {
    this->write(reinterpret_cast<const char*>(&v), sizeof(v));
  }

  // TODO: These should use the le_ types if we ever build this on big-endian systems
  inline void put_u8(uint8_t v) { this->data.push_back(static_cast<char>(v)); }
  inline void put_s8(int8_t v) { this->data.push_back(v); }
  inline void put_u16l(uint16_t v) { this->put<uint16_t>(v); }
  inline void put_s16l(int16_t v) { this->put<int16_t>(v); }
  inline void put_u32l(uint32_t v) { this->put<uint32_t>(v); }
  inline void put_s32l(int32_t v) { this->put<int32_t>(v); }
  inline void put_u64l(uint64_t v) { this->put<uint64_t>(v); }
  inline void put_s64l(int64_t v) { this->put<int64_t>(v); }
  inline void put_f32l(float v) { this->put<float>(v); }
  inline void put_f64l(double v) { this->put<double>(v); }

  inline std::string& str() {
    return this->data;
  }
  inline const std::string& str() const {
    return this->data;
  }

private:
  std::string data;
};

////////////////////////////////////////////////////////////////////////////////
// Object references

// This class holds a reference to a PyObject. When constructed, this class
// takes ownership of the passed-in reference (the caller should NOT call
// Py_DECREF on it). This can be thought of as an analogue to std::shared_ptr,
// but for Python objects.
// NOTE: There is no operator PyObject*, and this is intentional - we want the
// caller to have to think about whether they want to borrow the reference or
// make a new reference, so they must call .borrow() or .new_ref() to get it.
template <typename TargetT = PyObject>
struct PyObjectRef {
  TargetT* obj;

  PyObjectRef() : obj(nullptr) {}
  PyObjectRef(TargetT* obj) : obj(obj) {}
  ~PyObjectRef() {
    this->clear();
  }

  // Technically these could be made copyable, but we don't really need that
  // functionality, and leaving the copy constructors deleted allows us to
  // detect extraneous increfs/decrefs.
  PyObjectRef(const PyObjectRef& other) = delete;
  PyObjectRef& operator=(const PyObjectRef& other) = delete;

  PyObjectRef(PyObjectRef&& other) : obj(other.obj) {
    other.obj = nullptr;
  }
  PyObjectRef& operator=(PyObjectRef&& other) {
    PyObject* prev_obj = this->obj;
    this->obj = other.obj;
    other.obj = nullptr;
    Py_XDECREF(prev_obj);
    return *this;
  }

  operator bool() const {
    return !!this->obj;
  }
  void assign_ref(TargetT* obj) {
    TargetT* prev_obj = this->obj;
    this->obj = obj;
    Py_XDECREF(prev_obj);
  }
  TargetT* borrow() const {
    return this->obj;
  }
  TargetT* new_ref() const {
    Py_INCREF(this->obj);
    return this->obj;
  }
  TargetT* release() {
    TargetT* ret = this->obj;
    this->obj = nullptr;
    return ret;
  }
  TargetT* operator->() const {
    return this->obj;
  }
  inline void clear() {
    this->assign_ref(nullptr);
  }
};
static_assert(sizeof(PyObjectRef<>) == sizeof(PyObject*), "PyObjectRef contains more than just a single pointer");

static std::string repr(PyObject* obj) {
  PyObjectRef<> repr = raise_python_errors(PyObject_Repr, obj);
  if (!PyUnicode_Check(repr.borrow())) {
    throw std::runtime_error("repr() returned something other than a unicode object");
  }
  return std::string(PyUnicode_AsUTF8(repr.borrow()));
}

////////////////////////////////////////////////////////////////////////////////
// Enums

// This class holds a reference to a constructed Python Enum class and allows
// fast native lookups of the values and Python value objects. This should be
// subclassed and instantiated once for each enum the caller intends to use.
class PyEnumRef {
public:
  void create_py_enum() {
    PyObjectRef<> enum_module = raise_python_errors(PyImport_ImportModule, "enum");
    // NOTE: We intentionally don't use IntEnum here, to prevent users from
    // accidentally assigning an enum value to an int-valued field.
    PyObjectRef<> enum_class = raise_python_errors(PyObject_GetAttrString, enum_module.borrow(), "IntEnum");
    PyObjectRef<> enum_name = raise_python_errors(PyUnicode_FromString, this->get_python_name());
    PyObjectRef<> enum_members = raise_python_errors(PyDict_New);

    this->populate_values(enum_members.borrow());

    PyObjectRef<> args = raise_python_errors(PyTuple_Pack, 2, enum_name.borrow(), enum_members.borrow());
    PyObjectRef<> local_py_enum = raise_python_errors(PyObject_CallObject, enum_class.borrow(), args.borrow());

    // Populate the values map by iterating the constructed enum class
    PyObjectRef<> it = raise_python_errors(PyObject_GetIter, local_py_enum.borrow());
    while (PyObjectRef<> entry = PyIter_Next(it.borrow())) {
      PyObjectRef<> enum_value = raise_python_errors(PyObject_GetAttrString, entry.borrow(), "value");

      // Enum values can be negative in protobuf, and they just get encoded
      // as unsigned 32-bit integers anyway (which is inefficient). To
      // implement this behavior, we treat the Python integer as signed, then
      // immediately discard its sign information.
      int64_t value = PyLong_AsLongLong(enum_value.borrow());
      if (!is_in_s32_range(value)) {
        throw std::runtime_error("Enum value outside of signed 32-bit range");
      } else if ((value != -1) || !PyErr_Occurred()) {
        auto& ref = this->py_enum_value_for_int_value[value];
        ref.assign_ref(entry.release());
        this->int_value_for_py_enum_value.emplace(ref.borrow(), value);
      } else {
        throw python_error("");
      }
    }
    if (PyErr_Occurred()) {
      throw python_error("");
    }

    // It seems the enum members can't be pickled because the pickler can't
    // look up which module they're in (it appears as importlib._bootstrap)
    // unless we do this.
    PyObjectRef<> module_name_str = PyUnicode_FromString("__COMPILER__QUALIFIED_MODULE_NAME__");
    PyObject_SetAttrString(local_py_enum.borrow(), "__module__", module_name_str.borrow());

    this->py_enum.assign_ref(local_py_enum.release());
  }

  const PyObjectRef<>& py_enum_class() {
    return this->py_enum;
  }
  bool has_py_member(const PyObject* obj) const {
    return this->int_value_for_py_enum_value.count(obj);
  }
  const PyObjectRef<>& py_member_for_value(int64_t value) const {
    try {
      return this->py_enum_value_for_int_value.at(value);
    } catch (const std::out_of_range&) {
      throw std::runtime_error(string_printf("Enum member %" PRIu64 " does not exist", value));
    }
  }
  int64_t value_for_py_member(const PyObject* obj) const {
    try {
      return this->int_value_for_py_enum_value.at(obj);
    } catch (const std::out_of_range&) {
      throw std::runtime_error("Value is not an enum member");
    }
  }

protected:
  virtual const char* get_python_name() const = 0;
  virtual void populate_values(PyObject* dict) = 0;

private:
  std::unordered_map<const PyObject*, int64_t> int_value_for_py_enum_value;
  std::unordered_map<int64_t, PyObjectRef<>> py_enum_value_for_int_value;
  PyObjectRef<> py_enum;
};

// __COMPILER__FOREACH_MODULE__
// __COMPILER__FOREACH_ENUM__
class __COMPILER__ENUM_CC_NAME__EnumRef : public PyEnumRef {
protected:
  virtual const char* get_python_name() const {
    return "__COMPILER__MODULE_NAME__.__COMPILER__ENUM_PYTHON_NAME__";
  }
  virtual void populate_values(PyObject* dict) {
    // __COMPILER__FOREACH_ENUM_MEMBER__
    {
      PyObjectRef<> __COMPILER__ENUM_MEMBER_NAME___value = raise_python_errors(PyLong_FromLong, __COMPILER__ENUM_MEMBER_VALUE__);
      if (PyDict_SetItemString(dict, "__COMPILER__ENUM_MEMBER_NAME__", __COMPILER__ENUM_MEMBER_NAME___value.borrow())) {
        throw python_error("");
      }
    }
    // __COMPILER__END_FOREACH__
  }
};

__COMPILER__ENUM_CC_NAME__EnumRef __COMPILER__ENUM_CC_NAME___enum_ref;
// __COMPILER__END_FOREACH__
// __COMPILER__END_FOREACH__

///////////////////////////////////////////////////////////////////////////////
// Python C API utility functions

static PyObject* py_dict_value_for_primitive_value(PyObject* obj) {
  // Hack: If the object has a .as_dict() method, call it and use the result.
  // Otherwise, just use the object itself.
  int has_as_dict = PyObject_HasAttrString(obj, "as_dict");
  if (has_as_dict == -1) {
    throw python_error("");
  }
  if (has_as_dict) {
    PyObjectRef<> method = raise_python_errors(PyObject_GetAttrString, obj, "as_dict");
    // args must not be null (so we use an empty tuple) but kwargs can be
    // null, according to the docs
    PyObjectRef<> args = raise_python_errors(PyTuple_New, 0);
    return raise_python_errors(PyObject_Call, method.borrow(), args.borrow(), nullptr);
  } else {
    Py_INCREF(obj);
    return obj;
  }
}

static PyObject* py_dict_value_for_value(PyObject* obj) {
  if (PyList_Check(obj)) {
    ssize_t num_items = PyList_Size(obj);
    if (num_items < 0) {
      throw python_error("");
    }
    PyObjectRef<> ret = raise_python_errors(PyList_New, num_items);
    for (ssize_t z = 0; z < num_items; z++) {
      PyList_SET_ITEM(ret.borrow(), z, py_dict_value_for_value(PyList_GET_ITEM(obj, z)));
    }
    return ret.release();

  } else if (PyDict_Check(obj)) {
    PyObjectRef<> ret = raise_python_errors(PyDict_New);
    // key and value will be borrowed references, so we don't have to DECREF them
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    // TODO: In the free-threaded build, we'll need PY_BEGIN_CRITICAL_SECTION
    // here, but that macro isn't (yet?) compatible with C++. See
    // https://docs.python.org/3/c-api/dict.html#c.PyDict_Next
    while (PyDict_Next(obj, &pos, &key, &value)) {
      PyObjectRef<> new_value = py_dict_value_for_value(value);
      PyDict_SetItem(ret.borrow(), key, new_value.borrow());
    }
    return ret.release();

  } else {
    return py_dict_value_for_primitive_value(obj);
  }
}

///////////////////////////////////////////////////////////////////////////////
// Protobuf definitions

enum class WireType {
  UNKNOWN = -1,

  // Field contents are another varint-encoded integer, zigzag-encoded if the
  // type is signed (sint32 or sint64).
  // Used for int32, int64, uint32, uint64, sint32, sint64, bool, enum
  VARINT = 0,
  // Field contents are 8 bytes, little-endian.
  // Used for fixed64, sfixed64, double
  INT64 = 1,
  // Field contents are a varint specifying how many data bytes follow,
  // followed immediately by the data bytes. The number of items in a packed
  // repeated field is not specified; the parser should continue parsing items
  // until it reads the entire data string.
  // Used for string, bytes, embedded messages, packed repeated fields
  LENGTH = 2,
  // We don't support groups, since they're deprecated.
  GROUP_START = 3,
  GROUP_END = 4,
  // Field contents are 4 bytes, little-endian.
  // Used for fixed32, sfixed32, float
  INT32 = 5,
};

const char* name_for_wire_type(WireType t) {
  switch (t) {
    case WireType::VARINT:
      return "VARINT";
    case WireType::INT64:
      return "INT64";
    case WireType::LENGTH:
      return "LENGTH";
    case WireType::GROUP_START:
      return "GROUP_START";
    case WireType::GROUP_END:
      return "GROUP_END";
    case WireType::INT32:
      return "INT32";
    default:
      return "__UNKNOWN__";
  }
}

enum class DataType {
  UNKNOWN = -1,
  FLOAT,
  DOUBLE,
  INT32,
  UINT32,
  SINT32,
  INT64,
  UINT64,
  SINT64,
  FIXED32,
  SFIXED32,
  FIXED64,
  SFIXED64,
  BOOL,
  ENUM,
  STRING,
  BYTES,
  MAP, // message_constructor required in parse()
  MESSAGE, // message_constructor required in parse()
};

constexpr bool is_uint_data_type(DataType t) {
  return ((t == DataType::UINT32) ||
      (t == DataType::UINT64) ||
      (t == DataType::BOOL) ||
      (t == DataType::ENUM) ||
      (t == DataType::FIXED32) ||
      (t == DataType::FIXED64));
}
constexpr bool is_sint_data_type(DataType t) {
  return ((t == DataType::INT32) ||
      (t == DataType::SINT32) ||
      (t == DataType::INT64) ||
      (t == DataType::SINT64) ||
      (t == DataType::SFIXED32) ||
      (t == DataType::SFIXED64));
}
constexpr bool is_float_data_type(DataType t) {
  return ((t == DataType::FLOAT) ||
      (t == DataType::DOUBLE));
}

constexpr bool is_varint_data_type(DataType t) {
  return ((t == DataType::INT32) ||
      (t == DataType::UINT32) ||
      (t == DataType::SINT32) ||
      (t == DataType::INT64) ||
      (t == DataType::UINT64) ||
      (t == DataType::SINT64) ||
      (t == DataType::BOOL) ||
      (t == DataType::ENUM));
}
constexpr bool is_int32_data_type(DataType t) {
  return ((t == DataType::FLOAT) ||
      (t == DataType::FIXED32) ||
      (t == DataType::SFIXED32));
}
constexpr bool is_int64_data_type(DataType t) {
  return ((t == DataType::DOUBLE) ||
      (t == DataType::FIXED64) ||
      (t == DataType::SFIXED64));
}
constexpr bool is_string_data_type(DataType t) {
  return ((t == DataType::STRING) ||
      (t == DataType::BYTES));
}
constexpr bool is_message_data_type(DataType t) {
  return ((t == DataType::MAP) ||
      (t == DataType::MESSAGE));
}
constexpr bool can_use_packed_repeated_format(DataType t) {
  // String data types can't be serialized in the packed format, since they
  // couldn't be distinguished on the wire from non-packed string data types if
  // that were allowed
  return (!is_string_data_type(t) && (t != DataType::MAP) && (t != DataType::MESSAGE));
}

template <DataType data_type>
  requires(is_uint_data_type(data_type) && data_type != DataType::BOOL && data_type != DataType::ENUM)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyLong_Check(obj)) {
    return false;
  }
  uint64_t ret = PyLong_AsUnsignedLongLong(obj);
  if (ret == 0) {
    return true;
  } else if ((ret != static_cast<uint64_t>(-1)) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_sint_data_type(data_type))
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyLong_Check(obj)) {
    return false;
  }
  int64_t ret = PyLong_AsLongLong(obj);
  if (ret == 0) {
    return true;
  } else if ((ret != -1) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_float_data_type(data_type))
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyFloat_Check(obj) && !PyLong_Check(obj)) {
    return false;
  }
  double ret = PyFloat_AsDouble(obj);
  if (ret == 0.0) {
    return true;
  } else if ((ret != -1.0) || !PyErr_Occurred()) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(data_type == DataType::BOOL)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  return PyBool_Check(obj) && Py_IsFalse(obj);
}
template <DataType data_type>
  requires(data_type == DataType::ENUM)
bool obj_has_default_value(PyObject* obj, const PyEnumRef* enum_ref) {
  try {
    return (enum_ref->value_for_py_member(obj) == 0);
  } catch (const std::runtime_error&) {
    return false;
  }
}
template <DataType data_type>
  requires(data_type == DataType::STRING)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyUnicode_Check(obj)) {
    return false;
  }
  ssize_t length = PyUnicode_GetLength(obj);
  if (length == 0) {
    return true;
  } else if (length != -1.0) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(data_type == DataType::BYTES)
bool obj_has_default_value(PyObject* obj, const PyEnumRef*) {
  if (!PyBytes_Check(obj)) {
    return false;
  }
  ssize_t length = PyBytes_Size(obj);
  if (length == 0) {
    return true;
  } else if (length != -1.0) {
    return false;
  } else {
    throw python_error("");
  }
}
template <DataType data_type>
  requires(is_message_data_type(data_type))
bool obj_has_default_value(PyObject*, const PyEnumRef*) {
  // TODO: This shouldn't always return false. This shouldn't cause any
  // correctness issues but probably causes us to waste some space.
  return false;
}

static constexpr WireType wire_type_for_data_type(DataType t) {
  switch (t) {
    case DataType::FIXED32:
    case DataType::SFIXED32:
    case DataType::FLOAT:
      return WireType::INT32;
    case DataType::FIXED64:
    case DataType::SFIXED64:
    case DataType::DOUBLE:
      return WireType::INT64;
    case DataType::INT32:
    case DataType::UINT32:
    case DataType::SINT32:
    case DataType::INT64:
    case DataType::UINT64:
    case DataType::SINT64:
    case DataType::BOOL:
    case DataType::ENUM:
      return WireType::VARINT;
    case DataType::STRING:
    case DataType::BYTES:
    case DataType::MAP:
    case DataType::MESSAGE:
      return WireType::LENGTH;
    default:
      return WireType::UNKNOWN;
  }
}

static inline WireType wire_type_for_tag(uint64_t tag) {
  return static_cast<WireType>(tag & 7);
}
static inline uint64_t field_num_for_tag(uint64_t tag) {
  return tag >> 3;
}
static inline uint64_t encode_tag(uint64_t field_num, WireType type) {
  return (field_num << 3) | static_cast<uint64_t>(type);
}

static uint64_t decode_varint(StringReader& r) {
  uint8_t shift = 0;
  uint64_t ret = 0;
  for (;;) {
    if (shift >= 64) {
      throw std::runtime_error("varint has more than 10 7-bit digits");
    }
    uint8_t v = r.get_u8();
    ret |= (static_cast<uint64_t>(v & 0x7F) << shift);
    if (!(v & 0x80)) {
      return ret;
    }
    shift += 7;
  }
}

void encode_varint(StringWriter& w, uint64_t v) {
  while (v > 0x7F) {
    w.put_u8((v & 0x7F) | 0x80);
    v >>= 7;
  }
  // v cannot be zero here unless it was already zero before the loop
  w.put_u8(v);
}

int64_t decode_varint_signed(StringReader& r) {
  uint64_t v = decode_varint(r);
  return (v >> 1) ^ ((v & 1) ? -1 : 0);
}
void encode_varint_signed32(StringWriter& w, int32_t n) {
  encode_varint(w, static_cast<uint32_t>((n << 1) ^ (n >> 31)));
}
void encode_varint_signed64(StringWriter& w, int64_t n) {
  encode_varint(w, (n << 1) ^ (n >> 63));
}

///////////////////////////////////////////////////////////////////////////////
// Field codecs

enum ParseFlag {
  RETAIN_UNKNOWN_FIELDS = 0x01,
  IGNORE_INCORRECT_TYPES = 0x02,
};

using ParseMessageFn = PyObject* (*)(const void* data, size_t size, uint8_t flags);
using SerializeMessageFn = void (*)(PyObject* obj, StringWriter&);

[[noreturn]] void throw_incorrect_type(WireType expected_type, WireType received_type) {
  throw std::runtime_error(string_printf(
      "Incorrect type: expected %s, received %s",
      name_for_wire_type(expected_type), name_for_wire_type(received_type)));
}

static PyObjectRef<> py_int_zero;
static PyObjectRef<> py_float_zero;
static PyObjectRef<> py_empty_str;
static PyObjectRef<> py_empty_bytes;

PyObject* create_py_none() {
  Py_RETURN_NONE;
}
PyObject* create_py_false() {
  Py_RETURN_FALSE;
}
PyObject* create_py_int_zero() {
  return raise_python_errors(PyLong_FromLong, 0);
}
PyObject* create_py_float_zero() {
  return raise_python_errors(PyFloat_FromDouble, 0.0);
}
PyObject* create_py_empty_str() {
  return raise_python_errors(PyUnicode_FromStringAndSize, nullptr, 0);
}
PyObject* create_py_empty_bytes() {
  return raise_python_errors(PyBytes_FromStringAndSize, nullptr, 0);
}
PyObject* create_py_empty_list() {
  return raise_python_errors(PyList_New, 0);
}
PyObject* create_py_empty_dict() {
  return raise_python_errors(PyDict_New);
}

// The following three functions are templates, specialized (via `requires`)
// for each data type.

template <DataType...>
struct AlwaysFalse {
  static constexpr bool v = false;
};

template <DataType data_type>
struct TypeCodec {
  static bool value_matches_type(PyObject*, PyEnumRef*, PyTypeObject*, bool) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::value_matches_type should never be called");
    return false;
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::construct_default should never be called");
    return nullptr;
  }
  static PyObject* parse(StringReader&, PyEnumRef*, ParseMessageFn, uint8_t) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::parse should never be called");
    return nullptr;
  }
  static void serialize_without_tag(StringWriter&, PyObject*, PyEnumRef*, SerializeMessageFn) {
    static_assert(AlwaysFalse<data_type>::v, "Unspecialized TypeCodec::serialize_without_tag should never be called");
  }
};

template <>
struct TypeCodec<DataType::INT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, static_cast<int32_t>(decode_varint(r)));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of signed 32-bit range");
    }
    // Note: It appears Google's protobuf library encodes this as if it were a
    // 64-bit integer, so -1 is encoded as 10 bytes instead of 5 bytes. We do
    // the same here, even though it's probably wrong.
    encode_varint(w, static_cast<uint64_t>(v));
  }
};

template <>
struct TypeCodec<DataType::UINT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_u32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    encode_varint(w, v);
  }
};

template <>
struct TypeCodec<DataType::SINT32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, decode_varint_signed(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of signed 32-bit range");
    }
    encode_varint_signed32(w, v);
  }
};

template <>
struct TypeCodec<DataType::INT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, static_cast<int64_t>(decode_varint(r)));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint(w, static_cast<uint64_t>(v));
  }
};

template <>
struct TypeCodec<DataType::UINT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLongLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint(w, v);
  }
};

template <>
struct TypeCodec<DataType::SINT64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, decode_varint_signed(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    encode_varint_signed64(w, v);
  }
};

template <>
struct TypeCodec<DataType::FIXED32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLong, r.get_u32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_u32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    w.put_u32l(v);
  }
};

template <>
struct TypeCodec<DataType::SFIXED32> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLong, r.get_s32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      throw python_error("");
    }
    if (!is_in_s32_range(v)) {
      throw std::runtime_error("Integer value out of unsigned 32-bit range");
    }
    w.put_s32l(v);
  }
};

template <>
struct TypeCodec<DataType::FIXED64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromUnsignedLongLong, r.get_u64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    uint64_t v = PyLong_AsUnsignedLongLong(obj);
    if (v == static_cast<uint64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_u64l(v);
  }
};

template <>
struct TypeCodec<DataType::SFIXED64> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_int_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyLong_FromLongLong, r.get_s64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    int64_t v = PyLong_AsLongLong(obj);
    if (v == static_cast<int64_t>(-1) && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_s64l(v);
  }
};

template <>
struct TypeCodec<DataType::BOOL> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyBool_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_false();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyBool_FromLong, decode_varint(r));
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    if (obj == Py_True) {
      w.put_u8(0x01);
    } else if (obj == Py_False) {
      w.put_u8(0x00);
    } else {
      throw std::invalid_argument("Boolean value was neither True nor False");
    }
  }
};

template <>
struct TypeCodec<DataType::FLOAT> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyFloat_Check(obj) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_float_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyFloat_FromDouble, r.get_f32l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    float v = PyFloat_AsDouble(obj);
    if (v == -1.0 && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_f32l(v);
  }
};

template <>
struct TypeCodec<DataType::DOUBLE> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyFloat_Check(obj) || PyLong_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_float_zero();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    return raise_python_errors(PyFloat_FromDouble, r.get_f64l());
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    double v = PyFloat_AsDouble(obj);
    if (v == -1.0 && PyErr_Occurred()) {
      throw python_error("");
    }
    w.put_f64l(v);
  }
};

template <>
struct TypeCodec<DataType::BYTES> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyBytes_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_empty_bytes();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    uint64_t size = decode_varint(r);
    return raise_python_errors(PyBytes_FromStringAndSize, reinterpret_cast<const char*>(r.getv(size)), size);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    char* data;
    ssize_t size;
    if (PyBytes_AsStringAndSize(obj, &data, &size)) {
      throw python_error("");
    }
    encode_varint(w, size);
    w.write(data, size);
  }
};

template <>
struct TypeCodec<DataType::STRING> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject*, bool is_optional) {
    return (is_optional && (obj == Py_None)) || PyUnicode_Check(obj);
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn) {
    return create_py_empty_str();
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn, uint8_t) {
    uint64_t size = decode_varint(r);
    return raise_python_errors(PyUnicode_FromStringAndSize, reinterpret_cast<const char*>(r.getv(size)), size);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn) {
    ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(obj, &size);
    if (!data) {
      throw python_error("");
    }
    encode_varint(w, size);
    w.write(data, size);
  }
};

template <>
struct TypeCodec<DataType::ENUM> {
  static bool value_matches_type(PyObject* obj, PyEnumRef* enum_ref, PyTypeObject*, bool is_optional) {
    if (!enum_ref) {
      throw std::logic_error("Enum definition is missing");
    }
    return (is_optional && (obj == Py_None)) || enum_ref->has_py_member(obj);
  }
  static PyObject* construct_default(PyEnumRef* enum_ref, ParseMessageFn) {
    return enum_ref->py_member_for_value(0).new_ref();
  }
  static PyObject* parse(StringReader& r, PyEnumRef* enum_ref, ParseMessageFn, uint8_t) {
    if (!enum_ref) {
      throw std::logic_error("Enum definition is missing");
    }
    int64_t v = static_cast<int64_t>(decode_varint(r));
    return enum_ref->py_member_for_value(v).new_ref();
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef* enum_ref, SerializeMessageFn) {
    encode_varint(w, enum_ref->value_for_py_member(obj));
  }
};

template <>
struct TypeCodec<DataType::MESSAGE> {
  static bool value_matches_type(PyObject* obj, PyEnumRef*, PyTypeObject* type, bool is_optional) {
    if (is_optional && (obj == Py_None)) {
      return true;
    }
    int is_this_type = PyObject_IsInstance(obj, reinterpret_cast<PyObject*>(type));
    if (is_this_type == 1) {
      return true;
    } else if (is_this_type == 0) {
      return false;
    } else {
      throw python_error("");
    }
  }
  static PyObject* construct_default(PyEnumRef*, ParseMessageFn parse) {
    return parse(nullptr, 0, false);
  }
  static PyObject* parse(StringReader& r, PyEnumRef*, ParseMessageFn parse_message, uint8_t flags) {
    uint64_t size = decode_varint(r);
    if (!parse_message) {
      throw std::logic_error("Parser not available for submessage");
    }
    return parse_message(r.getv(size), size, flags);
  }
  static void serialize_without_tag(StringWriter& w, PyObject* obj, PyEnumRef*, SerializeMessageFn serialize_message) {
    if (!serialize_message) {
      throw std::logic_error("Serializer not available for submessage");
    }
    StringWriter sub_w;
    serialize_message(obj, sub_w);
    encode_varint(w, sub_w.size());
    w.write(sub_w.str());
  }
};

// Serializes a field AND its tag, unless its value is the default value

enum class DefaultBehavior {
  // Don't write anything if the value is None
  OPTIONAL = 0,
  // Don't write anything if the value is the default
  REQUIRED,
  // Always write the value, even if it's the default (this is needed to make
  // sure that default values in repeated fields are serialized properly))
  ALWAYS_WRITE,
};

template <DataType data_type>
void serialize_with_tag(StringWriter& w, uint64_t field_num, DefaultBehavior default_behavior, PyObject* obj, PyEnumRef* enum_ref, SerializeMessageFn serialize_message) {
  // Optional fields are typed as `X | None`. If it's None, serialize nothing.
  // Non-optional fields cannot be None, so serialize nothing if the field has
  // its default value.
  bool should_write;
  switch (default_behavior) {
    case DefaultBehavior::OPTIONAL:
      should_write = (obj != Py_None);
      break;
    case DefaultBehavior::REQUIRED:
      should_write = !obj_has_default_value<data_type>(obj, enum_ref);
      break;
    case DefaultBehavior::ALWAYS_WRITE:
      should_write = true;
      break;
    default:
      throw std::logic_error("invalid default behavior");
  }
  if (should_write) {
    encode_varint(w, encode_tag(field_num, wire_type_for_data_type(data_type)));
    TypeCodec<data_type>::serialize_without_tag(w, obj, enum_ref, serialize_message);
  }
}
template <>
void serialize_with_tag<DataType::MESSAGE>(StringWriter& w, uint64_t field_num, DefaultBehavior default_behavior, PyObject* obj, PyEnumRef*, SerializeMessageFn serialize_message) {
  if ((default_behavior == DefaultBehavior::OPTIONAL) && (obj == Py_None)) {
    return;
  }
  if (!serialize_message) {
    throw std::logic_error("Serializer not available for submessage");
  }
  StringWriter sub_w;
  serialize_message(obj, sub_w);
  if ((sub_w.size() == 0) && (default_behavior == DefaultBehavior::REQUIRED)) {
    // The submessage had no non-default values and is not optional; no need to
    // serialize anything
    return;
  }
  encode_varint(w, encode_tag(field_num, wire_type_for_data_type(DataType::MESSAGE)));
  encode_varint(w, sub_w.size());
  w.write(sub_w.str());
}

// Repeated field parsing/serializing

template <DataType data_type>
void parse_packed_repeated(PyObject* list, StringReader& r, PyEnumRef* enum_ref, ParseMessageFn parse_message, uint8_t flags) {
  // Get the length, then parse as many items as possible from the following
  // bytes and append them all to the list
  uint64_t size = decode_varint(r);
  StringReader sub_r = r.subx(r.where(), size);
  r.skip(size);
  while (!sub_r.eof()) {
    PyObjectRef<> v = TypeCodec<data_type>::parse(sub_r, enum_ref, parse_message, flags);
    if (PyList_Append(list, v.borrow())) {
      throw python_error("");
    }
  }
}

template <DataType data_type>
void parse_unpacked_repeated(PyObject* list, StringReader& r, PyEnumRef* enum_ref, ParseMessageFn parse_message, uint8_t flags) {
  // Parse a single item and append it to the list
  PyObjectRef<> v = TypeCodec<data_type>::parse(r, enum_ref, parse_message, flags);
  if (PyList_Append(list, v.borrow())) {
    throw python_error("");
  }
}

template <DataType data_type>
  requires(is_int32_data_type(data_type) || is_int64_data_type(data_type))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef*, SerializeMessageFn, PyTypeObject*) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  ssize_t num_items = PyList_Size(list);
  if (num_items == 0) {
    return;
  } else if (num_items < 0) {
    throw python_error("");
  }

  // Serialize in packed repeated format (LENGTH), with initially-known size
  encode_varint(w, encode_tag(field_num, WireType::LENGTH));
  size_t data_size = num_items * (is_int64_data_type(data_type) ? 8 : 4);
  encode_varint(w, data_size);

  size_t end_offset = w.size() + data_size;
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), nullptr, nullptr, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      TypeCodec<data_type>::serialize_without_tag(w, item.borrow(), nullptr, nullptr);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  } else if (end_offset != w.size()) {
    throw std::runtime_error("Serialized size does not match expected size");
  }
}
template <DataType data_type>
  requires(is_varint_data_type(data_type))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef* enum_ref, SerializeMessageFn, PyTypeObject*) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  ssize_t num_items = PyList_Size(list);
  if (num_items == 0) {
    return;
  } else if (num_items < 0) {
    throw python_error("");
  }

  // Serialize in packed repeated format (LENGTH), with initially-unknown size
  StringWriter items_w;
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), enum_ref, nullptr, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      TypeCodec<data_type>::serialize_without_tag(items_w, item.borrow(), enum_ref, nullptr);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  }

  encode_varint(w, encode_tag(field_num, WireType::LENGTH));
  encode_varint(w, items_w.size());
  w.write(items_w.str());
}

template <DataType data_type>
  requires(is_string_data_type(data_type) || (data_type == DataType::MESSAGE))
void serialize_repeated_with_tag(StringWriter& w, uint64_t field_num, PyObject* list, PyEnumRef*, SerializeMessageFn serialize_message, PyTypeObject* py_message_type) {
  if (!PyList_Check(list)) {
    throw std::runtime_error("Value expected to be a list but it isn\'t");
  }

  // Serialize in standard (non-packed) repeated format
  PyObjectRef<> it = raise_python_errors(PyObject_GetIter, list);
  size_t index = 0;
  while (PyObjectRef<> item = PyIter_Next(it.borrow())) {
    try {
      if (!TypeCodec<data_type>::value_matches_type(item.borrow(), nullptr, py_message_type, false)) {
        throw std::runtime_error("Incorrect data type for field: " + repr(item.borrow()));
      }
      serialize_with_tag<data_type>(w, field_num, DefaultBehavior::ALWAYS_WRITE, item.borrow(), nullptr, serialize_message);
    } catch (const python_error& e) {
      throw python_error(string_printf("(Index:%zu) ", index) + e.what());
    } catch (const std::exception& e) {
      throw std::runtime_error(string_printf("(Index:%zu) ", index) + e.what());
    }
    index++;
  }
  if (PyErr_Occurred()) {
    throw python_error("");
  }
}

// Map field parsing/serializing

template <DataType key_type, DataType value_type>
void parse_map(
    PyObject* dict,
    StringReader& r,
    PyEnumRef* value_enum_ref,
    ParseMessageFn value_parse_message,
    uint8_t flags) {
  // We don't bother with "proper" message decoding here, since the key and
  // value types are known and there can only be two fields in the submessage.
  uint64_t size = decode_varint(r);
  StringReader sub_r = r.subx(r.where(), size);
  r.skip(size);
  PyObjectRef<> key, value;
  while (!sub_r.eof()) {
    uint64_t tag = decode_varint(sub_r);
    WireType wire_type = wire_type_for_tag(tag);
    uint64_t field_num = field_num_for_tag(tag);
    // TODO: It'd be nice to store unknown fields here due to incorrect types;
    // currently we always raise in such situations
    if (field_num == 1) {
      if (wire_type != wire_type_for_data_type(key_type)) {
        throw_incorrect_type(wire_type_for_data_type(key_type), wire_type);
      }
      key.assign_ref(TypeCodec<key_type>::parse(sub_r, nullptr, nullptr, flags));
    } else if (field_num == 2) {
      if (wire_type != wire_type_for_data_type(value_type)) {
        throw_incorrect_type(wire_type_for_data_type(value_type), wire_type);
      }
      value.assign_ref(TypeCodec<value_type>::parse(sub_r, value_enum_ref, value_parse_message, flags));
    }
  }
  // If either the key or value is missing, parse an empty string to construct the default value
  if (!key) {
    key.assign_ref(TypeCodec<key_type>::construct_default(nullptr, nullptr));
  }
  if (!value) {
    value.assign_ref(TypeCodec<value_type>::construct_default(value_enum_ref, value_parse_message));
  }
  if (PyDict_SetItem(dict, key.borrow(), value.borrow())) {
    throw python_error("");
  }
}
template <DataType key_type, DataType value_type>
void serialize_map_with_tag(
    StringWriter& w,
    uint64_t field_num,
    PyObject* dict,
    PyEnumRef* value_enum_ref,
    SerializeMessageFn value_serialize_message,
    PyTypeObject* py_value_message_type) {
  if (!PyDict_Check(dict)) {
    throw std::runtime_error("Value is not a dictionary");
  }

  // key and value will be borrowed references, so we don't have to DECREF them
  PyObject* key;
  PyObject* value;
  Py_ssize_t pos = 0;
  // TODO: In the free-threaded build, we'll need PY_BEGIN_CRITICAL_SECTION
  // here, but that macro isn't (yet?) compatible with C++. See
  // https://docs.python.org/3/c-api/dict.html#c.PyDict_Next
  while (PyDict_Next(dict, &pos, &key, &value)) {
    if (!TypeCodec<key_type>::value_matches_type(key, nullptr, nullptr, false)) {
      throw std::runtime_error("Incorrect data type for key field: " + repr(key));
    }
    if (!TypeCodec<value_type>::value_matches_type(value, value_enum_ref, py_value_message_type, false)) {
      throw std::runtime_error("Incorrect data type for value field: " + repr(value));
    }
    // Technically there should be a sub-message here, but we just cheese it
    // since it would be annoying to implement "properly". The message will
    // always have fields 1 (key) and 2 (value), according to official protobuf
    // documentation.
    StringWriter item_w;
    // Apparently Google's protobuf library always writes these fields, even if
    // they have the default values, so we do so here too.
    serialize_with_tag<key_type>(item_w, 1, DefaultBehavior::ALWAYS_WRITE, key, nullptr, nullptr);
    serialize_with_tag<value_type>(item_w, 2, DefaultBehavior::ALWAYS_WRITE, value, value_enum_ref, value_serialize_message);
    encode_varint(w, encode_tag(field_num, WireType::LENGTH));
    encode_varint(w, item_w.size());
    w.write(item_w.str());
  }
}

// Oneof serializing (parsing doesn't require any special logic, but for
// serializing, we have to use isinstance() to figure out what to serialize)

struct SerializeOneofParams {
  uint64_t field_num = 0;
  bool is_optional = false;
  PyEnumRef* enum_ref = nullptr;
  SerializeMessageFn serialize_message = nullptr;
  PyTypeObject* message_type_obj = nullptr;
};

// Recursive case: serialize it if it's the first type; if it's not, try the
// remaining types recursively
template <DataType data_type, DataType... RemainingTs>
void serialize_oneof_with_tag(StringWriter& w, PyObject* obj, const SerializeOneofParams* params) {
  if (TypeCodec<data_type>::value_matches_type(obj, params->enum_ref, params->message_type_obj, false)) {
    auto default_behavior = params->is_optional ? DefaultBehavior::OPTIONAL : DefaultBehavior::REQUIRED;
    serialize_with_tag<data_type>(w, params->field_num, default_behavior, obj, params->enum_ref, params->serialize_message);
  } else {
    serialize_oneof_with_tag<RemainingTs...>(w, obj, params + 1);
  }
}

// Base case: no types matched (the caller always puts UNKNOWN at the end of
// the template args)
template <>
void serialize_oneof_with_tag<DataType::UNKNOWN>(StringWriter&, PyObject*, const SerializeOneofParams*) {
  // Base case - no types matched
  throw std::runtime_error("Value for oneof field was not any of the expected types");
}

// Skip a field's data without parsing it
void skip_field(StringReader& r, WireType type) {
  switch (type) {
    case WireType::VARINT:
      decode_varint(r);
      break;
    case WireType::INT64:
      r.skip(8);
      break;
    case WireType::LENGTH:
      r.skip(decode_varint(r));
      break;
    case WireType::INT32:
      r.skip(4);
      break;
    default:
      throw std::runtime_error(string_printf("Unknown field type %02hhX", static_cast<uint8_t>(type)));
  }
}

///////////////////////////////////////////////////////////////////////////////
// Message implementations

// Messages are not required to appear in topologically-sorted order, so we
// forward-declare them all first before any implementations.

// __COMPILER__FOREACH_MODULE__
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

  // All methods prefixed with py_ are to be called by Python callers; all other
  // methods are to be called from C++ only.

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

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_free_constructor = nullptr;
// __COMPILER__END_FOREACH__
// __COMPILER__END_FOREACH__

// __COMPILER__FOREACH_MODULE__
// __COMPILER__FOREACH_MESSAGE__
__COMPILER__MESSAGE_CC_NAME__* __COMPILER__MESSAGE_CC_NAME__::new_with_default_values(PyTypeObject* type) {

  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(type->tp_alloc(type, 0));
  if (!self) {
    throw python_error("");
  }
  new (&self->data) __COMPILER__MESSAGE_CC_NAME__::MessageData();

  // Populate defaults for all fields
  // __COMPILER__FOREACH_MESSAGE_FIELD_GROUP__
  self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(__COMPILER__MESSAGE_FIELD_GROUP_DEFAULT_VALUE_CONSTRUCTOR__);
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
  PyObjectRef<__COMPILER__MESSAGE_CC_NAME__> new_obj = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_type.tp_alloc(&py_type, 0));
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
    new_obj->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(self->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.new_ref());
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

void __COMPILER__MESSAGE_CC_NAME__::handle_incorrect_type(StringReader& r, uint64_t tag, DataType expected_type, uint8_t flags) {
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
            this->data.py___COMPILER__MESSAGE_FIELD_GROUP_NAME__.assign_ref(TypeCodec<DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__>::parse(
                r,
                __COMPILER__MESSAGE_FIELD_ENUM_REF__,
                __COMPILER__MESSAGE_FIELD_MESSAGE_PARSE_FN__,
                flags));
          } else {
            this->handle_incorrect_type(r, tag, DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__, flags);
          }
          // __COMPILER__END_IF__
          // __COMPILER__IF_MESSAGE_FIELD_TYPE_REPEATED__
          if (can_use_packed_repeated_format(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__) && (received_type == WireType::LENGTH)) {
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
          static_assert(wire_type_for_data_type(DataType::__COMPILER__MESSAGE_FIELD_DATA_TYPE__) == WireType::LENGTH, "Map-valued field does not expect MESSAGE data type");
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
          auto prefix = string_printf("(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__#__COMPILER__MESSAGE_FIELD_NUMBER__+0x%zX) ", r.where());
          throw python_error(prefix + e.what());
        } catch (const std::exception& e) {
          auto prefix = string_printf("(Field:__COMPILER__MESSAGE_FIELD_GROUP_NAME__#__COMPILER__MESSAGE_FIELD_NUMBER__+0x%zX) ", r.where());
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
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y#|pp", kwarg_names_arg, &input_data, &input_size, &retain_unknown_fields, &ignore_incorrect_types)) {
    return nullptr;
  }

  uint8_t flags = ((retain_unknown_fields ? ParseFlag::RETAIN_UNKNOWN_FIELDS : 0) |
      (ignore_incorrect_types ? ParseFlag::IGNORE_INCORRECT_TYPES : 0));

  return handle_python_errors([&]() -> PyObject* {
    reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(self)->parse_proto_into_this(input_data, input_size, flags);
    Py_RETURN_NONE;
  });
}

__COMPILER__MESSAGE_CC_NAME__* __COMPILER__MESSAGE_CC_NAME__::from_proto_data(const void* data, size_t size, uint8_t flags) {
  PyObjectRef<__COMPILER__MESSAGE_CC_NAME__> self = __COMPILER__MESSAGE_CC_NAME__::new_with_default_values(&__COMPILER__MESSAGE_CC_NAME__::py_type);
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
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y#|pp", kwarg_names_arg, &input_data, &input_size, &retain_unknown_fields, &ignore_incorrect_types)) {
    return nullptr;
  }

  uint8_t flags = ((retain_unknown_fields ? ParseFlag::RETAIN_UNKNOWN_FIELDS : 0) |
      (ignore_incorrect_types ? ParseFlag::IGNORE_INCORRECT_TYPES : 0));

  return handle_python_errors(__COMPILER__MESSAGE_CC_NAME__::from_proto_data, input_data, input_size, flags);
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_reduce(PyObject* py_self) {
  // We have to use a free function as the constructor, since the pickle module
  // doesn't know what to do with our submodule structure. We instead just tell
  // it to call the free function, which directly delegates to the constructor.
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
  self->parse_proto_into_this(data, size, false);
  Py_RETURN_NONE;
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
  Py_RETURN_NONE;
}

PyObject* __COMPILER__MESSAGE_CC_NAME__::py_has_unknown_fields(PyObject* py_self) {
  auto* self = reinterpret_cast<__COMPILER__MESSAGE_CC_NAME__*>(py_self);
  if (self->data.unknown_fields.empty()) {
    Py_RETURN_FALSE;
  } else {
    Py_RETURN_TRUE;
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
    PyObjectRef<> args_str = PyUnicode_Join(separator.borrow(), tokens.borrow());
    return raise_python_errors(PyUnicode_FromFormat, "__COMPILER__BASE_MODULE_NAME__.__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME__(%S)", args_str.borrow());
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
    // If we're doing an NE comparison and anything returns True, we're (they
    // are not equal). Similarly, if we're doing EQ and anything returns False,
    // we're done.
    if (result.borrow() != (is_ne ? Py_False : Py_True)) {
      return result.release();
    }
  }
  // __COMPILER__END_FOREACH__

  // We get here if every NE comparison above returned False or every EQ
  // comparison returned True, so self and other are actually equal. Return
  // the appropriate boolean value.
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
    // Note: The double reinterpret_casts here essentially tell the compiler
    // that we know what we're doing and it's OK to lose the argument type
    // information. See the notes on PyMethodDef::ml_meth in Python's docs:
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
    PyVarObject_HEAD_INIT(nullptr, 0) "__COMPILER__QUALIFIED_MODULE_NAME__.__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME__", // tp_name
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
// __COMPILER__END_FOREACH__

// Module definition

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
    "__COMPILER__QUALIFIED_MODULE_NAME__", // m_name
    nullptr, // m_doc
    -1, // m_size
    module_methods, // m_methods
    nullptr, // m_reload
    nullptr, // m_traverse
    nullptr, // m_clear
    nullptr, // m_free
};

static void add_object(PyObject* base_module, const std::string& path, PyObject* obj) {
  Py_INCREF(obj);

  PyObject* parent = base_module;
  std::string attr_name = path;
  size_t dot_pos = attr_name.find('.');
  while (dot_pos != std::string::npos) {
    std::string parent_name = attr_name.substr(0, dot_pos);
    attr_name = attr_name.substr(dot_pos + 1);

    PyObject* next_parent = PyObject_GetAttrString(parent, parent_name.c_str());
    if (!next_parent) {
      throw python_error("");
    }
    parent = next_parent;
    dot_pos = attr_name.find('.');
  }

  if (PyModule_Check(parent)) {
    if (PyModule_AddObjectRef(parent, attr_name.c_str(), obj)) {
      throw python_error("");
    }
  } else {
    if (PyObject_SetAttrString(parent, attr_name.c_str(), obj)) {
      throw python_error("");
    }
  }
}

// Submodule definitions
// __COMPILER__FOREACH_MODULE__
static struct PyModuleDef __COMPILER__MODULE_NAME___module_def = {
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

extern "C" PyMODINIT_FUNC PyInit___COMPILER__BASE_MODULE_NAME__(void) {
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
    __COMPILER__ENUM_CC_NAME___enum_ref.create_py_enum();
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
    add_object(m.borrow(), "__COMPILER__MODULE_NAME__.__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__", reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
    {
      __COMPILER__MESSAGE_CC_NAME__::py_free_constructor = PyObject_GetAttrString(m.borrow(), "__construct____COMPILER__MESSAGE_CC_NAME__");
      if (!__COMPILER__MESSAGE_CC_NAME__::py_free_constructor) {
        throw python_error("");
      }
      Py_INCREF(__COMPILER__MESSAGE_CC_NAME__::py_free_constructor);
    }
    // __COMPILER__END_FOREACH__
    // __COMPILER__FOREACH_ENUM__
    add_object(m.borrow(), "__COMPILER__MODULE_NAME__.__COMPILER__ENUM_PYTHON_NAME_ESCAPED__", __COMPILER__ENUM_CC_NAME___enum_ref.py_enum_class().borrow());
    // __COMPILER__END_FOREACH__
    // __COMPILER__END_FOREACH__

    // Global aliases
    // __COMPILER__FOREACH_GLOBAL_MESSAGE_ALIAS__
    add_object(m.borrow(), "__COMPILER__MESSAGE_PYTHON_NAME_ESCAPED__", reinterpret_cast<PyObject*>(&__COMPILER__MESSAGE_CC_NAME__::py_type));
    // __COMPILER__END_FOREACH__
    // __COMPILER__FOREACH_GLOBAL_ENUM_ALIAS__
    add_object(m.borrow(), "__COMPILER__ENUM_PYTHON_NAME_ESCAPED__", __COMPILER__ENUM_CC_NAME___enum_ref.py_enum_class().borrow());
    // __COMPILER__END_FOREACH__

    // Release the module pointer. If anything above raises, the reference
    // won't be released here (and returned) and will instead be destroyed by
    // the PyObjectRef destructor, so memory won't be leaked in case of
    // exceptions
    return m.release();
  });
}
