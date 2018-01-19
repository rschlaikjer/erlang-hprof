-module(hprof_parser).
-behavior(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include("include/records.hrl").

% Public API
-export([
    parse_file/1,
    parse_binary/1,
    close/1,
    % Fetch a single string from the string table
    get_string/2,
    % Fetch all heap instances. Response is streamed.
    get_all_instances/1,
    % Fetch instances matching a specific class name. Streamed.
    get_instances_for_class/2,
    % Fetch a primitive array, by ID
    get_primitive_array/2
]).

% gen_server
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    % The HPROF binary data itseIf
    hprof_data :: binary(),

    % Basic information about the dump file
    heap_ref_size :: 4 | 8,
    dump_timestamp_ms :: pos_integer(),

    instances = [],

    % String table
    ets_strings :: reference(),
    % Reverse mapping of strings -> IDs
    ets_strings_reverse :: reference(),

    % Various data
    ets_class_load :: reference(),
    ets_stack_trace :: reference(),
    ets_stack_frame :: reference(),
    ets_object_array :: reference(),
    ets_primitive_array :: reference(),
    ets_class_dump :: reference(),
    ets_class_name_by_id :: reference(),

    % A map of all object IDs to their root type
    ets_roots :: reference(),

    % Tables for each of the root types
    ets_roots_unknown :: reference(),
    ets_roots_jni_global :: reference(),
    ets_roots_jni_local :: reference(),
    ets_roots_java_frame :: reference(),
    ets_roots_native_stack :: reference(),
    ets_roots_sticky_class :: reference(),
    ets_roots_thread_block :: reference(),
    ets_roots_monitor_used :: reference(),
    ets_roots_thread_object :: reference(),
    ets_roots_interned_string :: reference(),
    ets_roots_debugger :: reference(),
    ets_roots_vm_internal :: reference(),
    ets_roots_jni_monitor :: reference()
}).

%% Public API

parse_file(Filename) ->
    gen_server:start_link(?MODULE, [{file, Filename}], []).

parse_binary(Binary) when is_binary(Binary) ->
    gen_server:start_link(?MODULE, [{binary, Binary}], []).

close(Pid) when is_pid(Pid) ->
    call(Pid, close).

get_primitive_array(Pid, ObjectId) when is_pid(Pid) ->
    call(Pid, {get_primitive_array, ObjectId}).

get_string(Pid, StringId) when is_pid(Pid) ->
    call(Pid, {get_string, StringId}).

get_all_instances(Pid) when is_pid(Pid) ->
    call(Pid, {get_all_instances, self()}).

get_instances_for_class(Pid, ClassName) when is_pid(Pid) and is_binary(ClassName) ->
    call(Pid, {get_instances_for_class, ClassName, self()}).

call(Pid, Data) -> gen_server:call(Pid, Data, infinity).

%% Callbacks

init([{file, Filename}]) ->
    gen_server:cast(self(), {parse_file, Filename}),
    {ok, init_ets(#state{})};
init([{binary, Binary}]) ->
    gen_server:cast(self(), {parse_binary, Binary}),
    {ok, init_ets(#state{})}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call({get_primitive_array, ObjectId}, _From, State) ->
    {reply, ets_get(State#state.ets_primitive_array, ObjectId), State};
handle_call({get_string, StringId}, _From, State) ->
    {reply, ets_get(State#state.ets_strings, StringId), State};
handle_call({get_all_instances, Caller}, _From, State) ->
    {ok, Ref} = stream_instances(State, Caller),
    {reply, {ok, Ref}, State};
handle_call({get_instances_for_class, ClassName, Caller}, _From, State) ->
    {ok, Ref} = stream_instances(State, Caller, ClassName),
    {reply, {ok, Ref}, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({parse_file, Filename}, State) ->
    State1 = parse_file_impl(State, Filename),
    {noreply, State1};
handle_cast({parse_binary, Binary}, State) ->
    State1 = parse_binary(State, Binary),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Implementation

stream_instances(State, Caller) ->
    stream_instances_filter(State, Caller, fun(_) -> true end).

stream_instances(State, Caller, ClassName) when is_binary(ClassName) ->
    % Get the string ID for the class name
    case get_id_for_string(State, ClassName) of
        not_found -> {error, class_name_not_found};
        StringId ->
            case get_class_obj_by_name_id(State, StringId) of
                not_found -> {error, class_obj_not_found};
                ClassObj ->
                    AcceptFun = fun(#hprof_heap_instance{class_object_id=Id}) ->
                        Id =:= ClassObj#hprof_class_dump.class_object
                    end,
                    stream_instances_filter(State, Caller, AcceptFun)
            end
    end.

stream_instances_filter(State, Caller, AcceptFun) when is_function(AcceptFun) ->
    % Create a reference to identify this request
    Ref = make_ref(),

    % Spawn a worker to iterate over the hprof data and pull out the relevant
    % instance data
    spawn_link(hprof_instance_parser, parse_instances, [
        self(), Caller, Ref, AcceptFun,
        State#state.hprof_data, State#state.ets_class_dump
    ]),

    % Return the request ref
    {ok, Ref}.

parse_file_impl(State, Filename) ->
    {ok, Bin} = file:read_file(Filename),
    parse_binary(State, Bin).

ets_get(Ets, Key) ->
    case ets:lookup(Ets, Key) of
        [] -> not_found;
        [Value] -> Value
    end.

init_ets(State) ->
    State#state{
        ets_strings = ets:new(strings, [set, {keypos, 2}]),
        ets_strings_reverse = ets:new(strings_reverse, [set]),
        ets_class_load = ets:new(class_load, [set, {keypos, 2}]),
        ets_stack_frame = ets:new(stack_frame, [set, {keypos, 2}]),
        ets_stack_trace = ets:new(stack_trace, [set, {keypos, 2}]),
        ets_object_array = ets:new(object_array, [set, {keypos, 2}, compressed]),
        ets_primitive_array = ets:new(primitive_array, [set, {keypos, 2}, compressed]),
        ets_class_dump = ets:new(class_dump, [set, {keypos, 2}]),
        ets_class_name_by_id = ets:new(class_dump, [set]),
        ets_roots = ets:new(roots, [set, {keypos, 2}]),
        ets_roots_unknown = ets:new(roots_unknown, [set, {keypos, 2}]),
        ets_roots_jni_global = ets:new(roots_jni_global, [set, {keypos, 2}]),
        ets_roots_jni_local = ets:new(roots_jni_local, [set, {keypos, 2}]),
        ets_roots_java_frame = ets:new(roots_java_frame, [set, {keypos, 2}]),
        ets_roots_native_stack = ets:new(roots_native_stack, [set, {keypos, 2}]),
        ets_roots_sticky_class = ets:new(roots_sticky_class, [set, {keypos, 2}]),
        ets_roots_thread_block = ets:new(roots_thread_block, [set, {keypos, 2}]),
        ets_roots_monitor_used = ets:new(roots_monitor_used, [set, {keypos, 2}]),
        ets_roots_thread_object = ets:new(roots_thread_object, [set, {keypos, 2}]),
        ets_roots_interned_string = ets:new(roots_interned_string, [set, {keypos, 2}]),
        ets_roots_debugger = ets:new(roots_debugger, [set, {keypos, 2}]),
        ets_roots_vm_internal = ets:new(roots_vm_internal, [set, {keypos, 2}]),
        ets_roots_jni_monitor = ets:new(roots_jni_monitor, [set, {keypos, 2}])
    }.

get_id_for_string(State, String) when is_binary(String) ->
    case ets:lookup(State#state.ets_strings_reverse, String) of
        [] -> not_found;
        [{_, Id}] -> Id
    end.

get_class_obj_by_name_id(State, StringId) ->
    % Need to use the class load records to get the class object ID
    case ets:select(
        State#state.ets_class_load,
        ets:fun2ms(fun(F=#hprof_record_load_class{class_name_string_id=Sid}) when StringId =:= Sid -> F end)) of
        [] -> not_found;
        [#hprof_record_load_class{class_object_id=ClsObjId}] ->
            case ets:lookup(State#state.ets_class_dump, ClsObjId) of
                [] -> not_found;
                [ClassDump] -> ClassDump
            end
    end.


parse_binary(State, Bin) ->
    parse_header(State#state{hprof_data=Bin}, Bin).

parse_header(State, Bindata) ->
    % Header has the format:
    % Fixed header (JAVA PROFILE 1.0.3)
    % u32: Pointer size (in bytes)
    % u64: Time this dump was taken (milliseconds)
    <<?HPROF_HEADER_MAGIC,
      HeapRefSize:?UINT32,
      DumpTimeMs:?UINT64,
      Rest/binary >> = Bindata,
    State1 = State#state{
        heap_ref_size = HeapRefSize,
        dump_timestamp_ms = DumpTimeMs
    },
    parse_records_optimized(State1, Rest).

parse_records_optimized(State, <<>>) ->
    State;
parse_records_optimized(State, <<Binary/binary>>) ->
    <<RecordType:?UINT8,
      _Microseconds:?UINT32,
      RecordSize:?UINT32,
      Rest/binary>> = Binary,
    parse_record_optimized(State, RecordSize, Rest, RecordType).

parse_record_optimized(State, Size, <<Binary/binary>>, ?HPROF_TAG_STRING) ->
    RefSize = State#state.heap_ref_size,
    StringLength = Size - RefSize,
    <<Id:RefSize/big-unsigned-integer-unit:8,
      Data:StringLength/binary,
      Rest/binary>> = Binary,
    % Wrap it in a record
    Record = #hprof_record_string{
        id=Id,
        data=Data
    },
    % Save it in the string table
    ets:insert(State#state.ets_strings, Record),
    % Also store a backmapping
    ets:insert(State#state.ets_strings_reverse, {
        Record#hprof_record_string.data,
        Record#hprof_record_string.id
    }),
    parse_records_optimized(State, Rest);
parse_record_optimized(State, _Size, <<Binary/binary>>, ?HPROF_TAG_LOAD_CLASS) ->
    % Loading a class
    % u32: Serial number
    % Ref: Class object ID
    % u32: Stack trace serial number
    % Ref: Class name string ID
    RefSize = State#state.heap_ref_size,
    <<Serial:?UINT32,
      ClassObjId:RefSize/big-unsigned-integer-unit:8,
      StackSerial:?UINT32,
      ClassNameId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary
    >> = Binary,
    Record = #hprof_record_load_class{
        serial=Serial,
        class_object_id=ClassObjId,
        stack_trace_serial=StackSerial,
        class_name_string_id=ClassNameId
    },
    ets:insert(State#state.ets_class_load, Record),
    parse_records_optimized(State, Rest);
parse_record_optimized(State, _Size, <<Binary/binary>>, ?HPROF_TAG_UNLOAD_CLASS) ->
    % Unloading a class
    % We don't really care about these records
    % u32: Serial number
    <<_Serial:?UINT32,
      Rest/binary
    >> = Binary,
    parse_records_optimized(State, Rest);
parse_record_optimized(State, _Size, <<Binary/binary>>, ?HPROF_TAG_STACK_FRAME) ->
    % Stack frame
    % Ref: Stack fram ID
    % Ref: Method name string ID
    % Ref: Method signature string ID
    % Ref: Source file name string ID
    % u32: Class serial number
    % u32: Location
    RefSize = State#state.heap_ref_size,
    <<FrameId:RefSize/big-unsigned-integer-unit:8,
      MethodNameStringId:RefSize/big-unsigned-integer-unit:8,
      MethodSigStringId:RefSize/big-unsigned-integer-unit:8,
      SourceFileStringId:RefSize/big-unsigned-integer-unit:8,
      ClassSerial:?UINT32,
      Location:?UINT32,
      Rest/binary
    >> = Binary,
    Record = #hprof_record_stack_frame{
        frame_id=FrameId,
        method_name_string_id=MethodNameStringId,
        method_signature_string_id=MethodSigStringId,
        source_file_string_id=SourceFileStringId,
        class_serial=ClassSerial,
        location=Location
    },
    ets:insert(State#state.ets_stack_frame, Record),
    parse_records_optimized(State, Rest);
parse_record_optimized(State, Size, <<Binary/binary>>, ?HPROF_TAG_STACK_TRACE) ->
    % Stack trace
    % u32: Stack trace serial
    % u32: Thread serial
    % u32: Number of frames
    % [Ref]: Stack fram IDs
    RefSize = State#state.heap_ref_size,
    FrameIdsSize = Size - 12,
    <<Serial:?UINT32,
      ThreadSerial:?UINT32,
      FrameCount:?UINT32,
      FrameIdsBin:FrameIdsSize/binary,
      Rest/binary
    >> = Binary,
    FrameIds = [
        FrameId || <<FrameId:RefSize/big-unsigned-integer-unit:8>>
        <= FrameIdsBin
    ],
    Record = #hprof_record_stack_trace{
        serial=Serial,
        thread_serial=ThreadSerial,
        frame_count=FrameCount,
        frame_ids=FrameIds
    },
    ets:insert(State#state.ets_stack_trace, Record),
    parse_records_optimized(State, Rest);
parse_record_optimized(State, Size, <<Binary/binary>>, ?HPROF_TAG_HEAP_DUMP_SEGMENT) ->
    parse_heap_dump_segments_optimized(State, Binary, Size);
parse_record_optimized(State, Size, <<Binary/binary>>, ?HPROF_TAG_HEAP_DUMP_END) ->
    % Nothing much doing here
    <<_:Size/binary, Rest/binary>> = Binary,
    parse_records_optimized(State, Rest).

parse_heap_dump_segments_optimized(State, <<Binary/binary>>, 0) ->
    parse_records_optimized(State, Binary);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_UNKNOWN, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_unknown{
        object_id=ObjectId
    },
    BytesRead = 1 + RefSize,
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=unknown
    }),
    ets:insert(State#state.ets_roots_unknown, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_GLOBAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      JniGlobalRefId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + (RefSize * 2),
    Root = #hprof_heap_root_jni_global{
        object_id=ObjectId,
        jni_global_ref_id=JniGlobalRefId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_global
    }),
    ets:insert(State#state.ets_roots_jni_global, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_LOCAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    Root = #hprof_heap_root_jni_local{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_local
    }),
    ets:insert(State#state.ets_roots_jni_local, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JAVA_FRAME, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    Root = #hprof_heap_root_java_frame{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=java_fram
    }),
    ets:insert(State#state.ets_roots_java_frame, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_NATIVE_STACK, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 4,
    Root = #hprof_heap_root_native_stack{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=native_stack
    }),
    ets:insert(State#state.ets_roots_native_stack, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_STICKY_CLASS, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    Root = #hprof_heap_root_sticky_class{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=sticky_class
    }),
    ets:insert(State#state.ets_roots_sticky_class, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_THREAD_BLOCK, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 4,
    Root = #hprof_heap_root_thread_block{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=thread_block
    }),
    ets:insert(State#state.ets_roots_thread_block, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_MONITOR_USED, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize,
    Root = #hprof_heap_root_monitor_used{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=monitor_used
    }),
    ets:insert(State#state.ets_roots_monitor_used, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_THREAD_OBJECT, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      StackTraceSerial:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    Root = #hprof_heap_root_thread_object{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        stack_trace_serial=StackTraceSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=thread_object
    }),
    ets:insert(State#state.ets_roots_thread_object, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_CLASS_DUMP, Bin/binary>>, RemainingBytes) ->
    parse_class_dump_segment_optimized(State, Bin, RemainingBytes - 1);
parse_heap_dump_segments_optimized(State, <<?HPROF_INSTANCE_DUMP, Bin/binary>>, RemainingBytes) ->
    % Can't actually parse these during the first pass, since they come *before*
    % the class definitions
    RefSize = State#state.heap_ref_size,
    <<_ObjectId:RefSize/big-unsigned-integer-unit:8,
      _StackTraceSerial:?UINT32,
      _ClassObjectId:RefSize/big-unsigned-integer-unit:8,
      DataSize:?UINT32,
      Rest/binary>> = Bin,
    <<_Data:DataSize/binary, Rest1/binary>> = Rest,
    BytesRead = 1 + RefSize + 4 + RefSize + 4 + DataSize,
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_OBJECT_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      ElementClassObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    DataSize = ElementCount * RefSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Rest,
    BytesRead = 1 + RefSize + 8 + RefSize + DataSize,
    Elements = [
        Elem || <<Elem:RefSize/big-unsigned-integer-unit:8>>
        <= ArrayData
    ],
    Array = #hprof_object_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_class_object_id=ElementClassObjectId,
        elements=Elements
    },
    ets:insert(State#state.ets_object_array, Array),
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_PRIMITIVE_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      DataType:?UINT8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 9,
    RefSize = State#state.heap_ref_size,
    ElementSize = primitive_size(RefSize, DataType),
    parse_primitive_array_dump(
        State, DataType, ElementCount, ElementSize, ObjectId,
        StackTraceSerial, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_HEAP_DUMP_INFO, Bin/binary>>, RemainingBytes) ->
    % Not currently tracking which heap things are in
    RefSize = State#state.heap_ref_size,
    <<_HeapType:?UINT32,
      _HeapNameStringId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    % Info = #hprof_heap_dump_info{
    %     heap_type=HeapType,
    %     heap_type_string_id=HeapNameStringId
    % },
    BytesRead = 1 + 4 + RefSize,
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_INTERNED_STRING, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_interned_string{
        object_id=ObjectId
    },
    BytesRead = 1 + RefSize,
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=interned_string
    }),
    ets:insert(State#state.ets_roots_interned_string, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_ROOT_FINALIZING, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_finalizing});
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_DEBUGGER, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_debugger{
        object_id=ObjectId
    },
    BytesRead = 1 + RefSize,
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=debugger
    }),
    ets:insert(State#state.ets_roots_debugger, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_ROOT_REFERENCE_CLEANUP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_reference_cleanup});
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_VM_INTERNAL, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_vm_internal{
        object_id=ObjectId
    },
    BytesRead = 1 + RefSize,
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=vm_internal
    }),
    ets:insert(State#state.ets_roots_vm_internal, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_MONITOR, Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 8,
    Root = #hprof_heap_root_jni_monitor{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_monitor
    }),
    ets:insert(State#state.ets_roots_jni_monitor, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_UNREACHABLE, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_unreachable});
parse_heap_dump_segments_optimized(_State, <<?HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_primitive_array_nodata_dump}).

parse_primitive_array_dump(State, DataType, ElementCount, ElementSize, ObjectId, StackTraceSerial, <<Binary/binary>>, RemainingBytes) ->
    DataSize = ElementCount * ElementSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Binary,
    Elements = [
        parse_primitive(Elem, DataType) || <<Elem:ElementSize/binary>>
        <= ArrayData
    ],
    Array = #hprof_primitive_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_type=DataType,
        elements=Elements
    },
    ets:insert(State#state.ets_primitive_array, Array),
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - DataSize).

primitive_size(RefSize, ?HPROF_BASIC_OBJECT) -> RefSize;
primitive_size(_, ?HPROF_BASIC_BOOLEAN) -> 1;
primitive_size(_, ?HPROF_BASIC_CHAR) -> 2;
primitive_size(_, ?HPROF_BASIC_FLOAT) -> 4;
primitive_size(_, ?HPROF_BASIC_DOUBLE) -> 8;
primitive_size(_, ?HPROF_BASIC_BYTE) -> 1;
primitive_size(_, ?HPROF_BASIC_SHORT) -> 2;
primitive_size(_, ?HPROF_BASIC_INT) -> 4;
primitive_size(_, ?HPROF_BASIC_LONG) -> 8.

parse_primitive(<<V:?UINT32>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT64>>, ?HPROF_BASIC_OBJECT) -> V;
parse_primitive(<<V:?UINT8>>, ?HPROF_BASIC_BOOLEAN) -> V;
parse_primitive(<<AH:1/binary, AL:1/binary>>, ?HPROF_BASIC_CHAR) -> <<AH/binary, AL/binary>>;
parse_primitive(<<255, 128, 0, 0>>, ?HPROF_BASIC_FLOAT) -> minus_infinity;
parse_primitive(<<127, 128, 0, 0>>, ?HPROF_BASIC_FLOAT) -> infinity;
parse_primitive(<<127, 192, 0, 0>>, ?HPROF_BASIC_FLOAT) -> nan;
parse_primitive(<<V:32/float>>, ?HPROF_BASIC_FLOAT) -> V;
parse_primitive(<<255,240,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> minus_infinity;
parse_primitive(<<127,248,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> infinity;
parse_primitive(<<127,240,0,0,0,0,0,0>>, ?HPROF_BASIC_DOUBLE) -> nan;
parse_primitive(<<V:64/float>>, ?HPROF_BASIC_DOUBLE) -> V;
parse_primitive(<<V:?INT8>>, ?HPROF_BASIC_BYTE) -> V;
parse_primitive(<<V:?INT16>>, ?HPROF_BASIC_SHORT) -> V;
parse_primitive(<<V:?INT32>>, ?HPROF_BASIC_INT) -> V;
parse_primitive(<<V:?INT64>>, ?HPROF_BASIC_LONG) -> V.

parse_class_dump_segment_optimized(State, <<Bin/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<ClassObjId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      SuperClassObjId:RefSize/big-unsigned-integer-unit:8,
      ClassLoaderObjId:RefSize/big-unsigned-integer-unit:8,
      Signer:RefSize/big-unsigned-integer-unit:8,
      ProtDomain:RefSize/big-unsigned-integer-unit:8,
      _Reserved1:RefSize/big-unsigned-integer-unit:8,
      _Reserved2:RefSize/big-unsigned-integer-unit:8,
      InstanceSize:?UINT32,
      NumConstants:?UINT16,
      Rest1/binary>> = Bin,

    HeaderBytesRead = RefSize + 4 + RefSize * 6 + 4 + 2,

    % Start building the class
    Class = #hprof_class_dump{
        class_object=ClassObjId,
        stack_trace_serial=StackTraceSerial,
        superclass_object=SuperClassObjId,
        classloader_object=ClassLoaderObjId,
        signer=Signer,
        prot_domain=ProtDomain,
        instance_size=InstanceSize,
        num_constants=NumConstants
    },

    % Empty constant pool for ART, apparently, but may as well be complete
    % about it.
    parse_class_dump_constants(
        State, Rest1, Class, RemainingBytes-HeaderBytesRead
    ).

parse_class_dump_constants(State, <<Binary/binary>>, Class=#hprof_class_dump{num_constants=NC}, RemainingBytes) ->
    parse_class_dump_constants(State, Binary, Class, NC, [], RemainingBytes).

parse_class_dump_constants(State, <<Binary/binary>>, Class, 0, Acc, RemainingBytes) ->
    parse_class_dump_segment_optimized_statics(
        State, Binary, Class#hprof_class_dump{constants=lists:reverse(Acc)}, RemainingBytes
    );
parse_class_dump_constants(State, <<Binary/binary>>, Class, NumConstants, Acc, RemainingBytes) ->
    <<ConstantPoolIndex:?UINT16,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    RefSize = State#state.heap_ref_size,
    FieldSize = primitive_size(RefSize, Type),
    parse_class_dump_constant(
        State, Rest, Class, NumConstants, Acc,
        RemainingBytes, ConstantPoolIndex, Type, FieldSize
    ).

parse_class_dump_constant(State, <<Binary/binary>>, Class, NumConstants, Acc,
                          RemainingBytes, ConstantPoolIndex, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_constant_field{
        constant_pool_index=ConstantPoolIndex,
        type=Type,
        data=FieldData
    },
    parse_class_dump_constants(
        State, Rest, Class, NumConstants-1, [Field|Acc],
        RemainingBytes - (2 + 1 + FieldSize)
    ).

parse_class_dump_segment_optimized_statics(State, <<Bin/binary>>, Class, RemainingBytes) ->
    %     static_fields=Statics,
    %     num_instance_fields=NumInstances,
    %     instance_fields=Instances
    % Static fields
    <<NumStatics:?UINT16, Rest/binary>> = Bin,
    parse_class_dump_static_fields(
        State, Rest, Class#hprof_class_dump{
            num_static_fields=NumStatics
        }, RemainingBytes - 2
    ).

parse_class_dump_static_fields(State, <<Binary/binary>>, Class=#hprof_class_dump{num_static_fields=NS}, RemainingBytes) ->
    parse_class_dump_static_fields(State, Binary, NS, [], Class, RemainingBytes).
parse_class_dump_static_fields(State, <<Binary/binary>>, 0, Acc, Class, RemainingBytes) ->
    parse_class_dump_segment_optimized_instances(
        State, Binary,
        Class#hprof_class_dump{static_fields=lists:reverse(Acc)},
        RemainingBytes
    );
parse_class_dump_static_fields(State, <<Binary/binary>>, NumStatics, Acc, Class, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(RefSize, Type),
    parse_class_dump_static(
        State, Rest, NumStatics, Acc, Class, RemainingBytes - (RefSize + 1),
        FieldNameStringId, Type, FieldSize
    ).

parse_class_dump_static(State, <<Binary/binary>>, NumStatics, Acc, Class, RemainingBytes, FieldNameStringId, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_static_field{
        name_string_id=FieldNameStringId,
        type=Type,
        data=FieldData
    },
    parse_class_dump_static_fields(
        State, Rest, NumStatics-1, [Field|Acc], Class,
        RemainingBytes - FieldSize
    ).

parse_class_dump_segment_optimized_instances(State, <<Bin/binary>>, Class, RemainingBytes) ->
    % Instance Fields
    <<NumInstances:?UINT16, Rest/binary>> = Bin,
    parse_class_dump_instance_fields(
        State, Rest, Class#hprof_class_dump{
            num_instance_fields=NumInstances
        }, RemainingBytes - 2
    ).

parse_class_dump_instance_fields(State, <<Binary/binary>>,
                                 Class=#hprof_class_dump{num_instance_fields=N},
                                 RemainingBytes) ->
    parse_class_dump_instance_fields(
        State, Binary, N, [], Class, RemainingBytes
    ).

parse_class_dump_instance_fields(State, <<Binary/binary>>, 0, Acc, Class, RemainingBytes) ->
    parse_class_dump_segment_finalize(
        State, Binary, Class#hprof_class_dump{
            instance_fields=lists:reverse(Acc)
        }, RemainingBytes
    );
parse_class_dump_instance_fields(State, <<Binary/binary>>, NumFields, Acc, Class, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    Field = #hprof_instance_field{
        name_string_id=FieldNameStringId,
        type=Type
    },
    parse_class_dump_instance_fields(
        State, Rest, NumFields-1, [Field|Acc],
        Class, RemainingBytes - (RefSize + 1)
    ).

parse_class_dump_segment_finalize(State, <<Bin/binary>>, Class, RemainingBytes) ->
    ets:insert(State#state.ets_class_dump, Class),
    parse_heap_dump_segments_optimized(State, Bin, RemainingBytes).
