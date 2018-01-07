%% Macros for decoding integers
% Big endian
-define(UINT8, 1/big-unsigned-integer-unit:8).
-define(UINT16, 2/big-unsigned-integer-unit:8).
-define(UINT32, 4/big-unsigned-integer-unit:8).
-define(UINT64, 8/big-unsigned-integer-unit:8).
% Little endian
-define(UINT16LE, 2/little-unsigned-integer-unit:8).
-define(UINT32LE, 4/little-unsigned-integer-unit:8).
-define(UINT64LE, 8/little-unsigned-integer-unit:8).

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
