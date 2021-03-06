%% Macros for decoding integers
% Signed
-define(INT8, 1/big-signed-integer-unit:8).
-define(INT16, 2/big-signed-integer-unit:8).
-define(INT32, 4/big-signed-integer-unit:8).
-define(INT64, 8/big-signed-integer-unit:8).
% Unsigned
-define(UINT8, 1/big-unsigned-integer-unit:8).
-define(UINT16, 2/big-unsigned-integer-unit:8).
-define(UINT32, 4/big-unsigned-integer-unit:8).
-define(UINT64, 8/big-unsigned-integer-unit:8).

%% Enum HprofTag
-define(HPROF_TAG_STRING, 16#01).
-define(HPROF_TAG_LOAD_CLASS, 16#02).
-define(HPROF_TAG_UNLOAD_CLASS, 16#03).
-define(HPROF_TAG_STACK_FRAME, 16#04).
-define(HPROF_TAG_STACK_TRACE, 16#05).
-define(HPROF_TAG_ALLOC_SITES, 16#06).
-define(HPROF_TAG_HEAP_SUMMARY, 16#07).
-define(HPROF_TAG_START_THREAD, 16#0A).
-define(HPROF_TAG_END_THREAD, 16#0B).
-define(HPROF_TAG_HEAP_DUMP, 16#0C).
-define(HPROF_TAG_HEAP_DUMP_SEGMENT, 16#1C).
-define(HPROF_TAG_HEAP_DUMP_END, 16#2C).
-define(HPROF_TAG_CPU_SAMPLES, 16#0D).
-define(HPROF_TAG_CONTROL_SETTINGS, 16#0E).

%% Enum HprofHeapTag
% Traditional
-define(HPROF_ROOT_UNKNOWN, 16#FF).
-define(HPROF_ROOT_JNI_GLOBAL, 16#01).
-define(HPROF_ROOT_JNI_LOCAL, 16#02).
-define(HPROF_ROOT_JAVA_FRAME, 16#03).
-define(HPROF_ROOT_NATIVE_STACK, 16#04).
-define(HPROF_ROOT_STICKY_CLASS, 16#05).
-define(HPROF_ROOT_THREAD_BLOCK, 16#06).
-define(HPROF_ROOT_MONITOR_USED, 16#07).
-define(HPROF_ROOT_THREAD_OBJECT, 16#08).
-define(HPROF_CLASS_DUMP, 16#20).
-define(HPROF_INSTANCE_DUMP, 16#21).
-define(HPROF_OBJECT_ARRAY_DUMP, 16#22).
-define(HPROF_PRIMITIVE_ARRAY_DUMP, 16#23).
% Android
-define(HPROF_HEAP_DUMP_INFO, 16#FE).
-define(HPROF_ROOT_INTERNED_STRING, 16#89).
-define(HPROF_ROOT_FINALIZING, 16#8A). % Obsolete
-define(HPROF_ROOT_DEBUGGER, 16#8B).
-define(HPROF_ROOT_REFERENCE_CLEANUP, 16#8C). % Obsolete
-define(HPROF_ROOT_VM_INTERNAL, 16#8D).
-define(HPROF_ROOT_JNI_MONITOR, 16#8E).
-define(HPROF_UNREACHABLE, 16#90). % Obsolete
-define(HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, 16#c3). % Obsolete

%% Enum HprofHeapId
-define(HPROF_HEAP_DEFAULT, 0).
-define(HPROF_HEAP_ZYGOTE, $Z).
-define(HPROF_HEAP_ZPP, $A).
-define(HPROF_HEAP_IMGAE, $I).

%% Enum HprofBasicType
-define(HPROF_BASIC_OBJECT, 2).
-define(HPROF_BASIC_BOOLEAN, 4).
-define(HPROF_BASIC_CHAR, 5).
-define(HPROF_BASIC_FLOAT, 6).
-define(HPROF_BASIC_DOUBLE, 7).
-define(HPROF_BASIC_BYTE, 8).
-define(HPROF_BASIC_SHORT, 9).
-define(HPROF_BASIC_INT, 10).
-define(HPROF_BASIC_LONG, 11).

% Header magic
-define(HPROF_HEADER_MAGIC, "JAVA PROFILE 1.0.3\0").

%% Records

-record(hprof_record_raw, {
    record_type :: pos_integer(),
    offset_microseconds :: pos_integer(),
    data_size :: pos_integer(),
    raw_data :: binary()
}).

-record(hprof_record_string, {
    id :: pos_integer(),
    data :: binary()
}).

-record(hprof_record_load_class, {
    serial :: pos_integer(),
    class_object_id :: pos_integer(),
    stack_trace_serial :: pos_integer(),
    class_name_string_id :: pos_integer()
}).

-record(hprof_record_unload_class, {
    serial :: pos_integer()
}).

-record(hprof_record_stack_frame, {
    frame_id :: pos_integer(),
    method_name_string_id :: pos_integer(),
    method_signature_string_id :: pos_integer(),
    source_file_string_id :: pos_integer(),
    class_serial :: pos_integer(),
    location :: pos_integer()
}).

-record(hprof_record_stack_trace, {
    serial :: pos_integer(),
    thread_serial :: pos_integer(),
    frame_count :: pos_integer(),
    frame_ids :: list(pos_integer())
}).

-record(hprof_record_heap_dump_segment, {
    segments :: list(any())
}).

-record(hprof_heap_root_unknown, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_jni_global, {
    object_id :: pos_integer(),
    jni_global_ref_id :: pos_integer()
}).

-record(hprof_heap_root_jni_local, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer(),
    frame_number :: pos_integer()
}).

-record(hprof_heap_root_java_frame, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer(),
    frame_number :: pos_integer()
}).

-record(hprof_heap_root_native_stack, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer()
}).

-record(hprof_heap_root_sticky_class, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_thread_block, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer()
}).

-record(hprof_heap_root_monitor_used, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_thread_object, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer(),
    stack_trace_serial :: pos_integer()
}).

-record(hprof_heap_instance_raw, {
    object_id :: pos_integer(),
    stack_trace_serial :: pos_integer(),
    class_object_id :: pos_integer(),
    data :: binary()
}).

-record(hprof_object_array, {
    object_id :: pos_integer(),
    stack_trace_serial :: pos_integer(),
    element_class_object_id :: pos_integer(),
    elements :: list(pos_integer())
}).

-record(hprof_primitive_array, {
    object_id :: pos_integer(),
    stack_trace_serial :: pos_integer(),
    element_type :: pos_integer(),
    elements :: list(any())
}).

-record(hprof_heap_dump_info, {
    heap_type :: pos_integer(),
    heap_type_string_id :: pos_integer()
}).

-record(hprof_heap_root_interned_string, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_debugger, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_vm_internal, {
    object_id :: pos_integer()
}).

-record(hprof_heap_root_jni_monitor, {
    object_id :: pos_integer(),
    thread_serial :: pos_integer(),
    frame_number :: pos_integer()
}).

-record(hprof_constant_field, {
    constant_pool_index :: pos_integer(),
    type :: pos_integer(),
    data :: any()
}).

-record(hprof_static_field, {
    name_string_id :: pos_integer(),
    type :: pos_integer(),
    data :: any()
}).

-record(hprof_instance_field, {
    name_string_id :: pos_integer(),
    name :: binary(),
    type :: atom(),
    value :: any()
}).

-record(hprof_class_dump, {
    class_id :: pos_integer(),
    stack_trace_serial :: pos_integer(),
    superclass_object :: pos_integer(),
    classloader_object :: pos_integer(),
    signer :: pos_integer(),
    prot_domain :: pos_integer(),
    instance_size :: pos_integer(),
    num_constants :: pos_integer(),
    constants :: list(#hprof_constant_field{}),
    num_static_fields :: pos_integer(),
    static_fields :: list(#hprof_static_field{}),
    num_instance_fields :: pos_integer(),
    instance_fields :: maps:map(#hprof_instance_field{})
}).

-record(hprof_heap_instance, {
    object_id :: pos_integer(),
    class_name :: binary(),
    stack_trace_serial :: pos_integer(),
    class_object_id :: pos_integer(),
    instance_values :: maps:map(#hprof_instance_field{})
}).

-record(hprof_root, {
    object_id,
    root_type
}).
