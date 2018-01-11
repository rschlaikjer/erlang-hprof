-module(hprof_parser).
-behavior(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include("include/records.hrl").

% Public API
-export([
    parse_file/1,
    parse_binary/1,
    close/1,
    get_primitive_arrays/1,
    print_class/2,
    get_string/2,
    get_instances_for_class/2,
    get_bitmaps/1,
    get_instance/2,
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
    % Basic information about the dump file
    heap_ref_size :: 4 | 8,
    dump_timestamp_ms :: pos_integer(),

    % List of instance records that need to be revisited and parsed
    raw_instance_records = [],

    % String table
    ets_strings :: reference(),
    % Reverse mapping of strings -> IDs
    ets_strings_reverse :: reference(),

    % Parsed instances
    ets_heap_instance :: reference(),

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

print_class(Pid, ClassId) ->
    Class = get_class_dump(Pid, ClassId),
    Statics = Class#hprof_class_dump.static_fields,
    Instances = Class#hprof_class_dump.instance_fields,
    StaticNames = [
        get_string(Pid, Sid) || #hprof_static_field{name_string_id=Sid}
        <- Statics
    ],
    InstanceFieldNames = [
        get_string(Pid, Sid) || #hprof_instance_field{name_string_id=Sid}
        <- Instances
    ],

    io:format("Class statics: ~p~n", [StaticNames]),
    io:format("Class instance: ~p~n", [InstanceFieldNames]).

parse_file(Filename) ->
    gen_server:start_link(?MODULE, [{file, Filename}], []).

parse_binary(Binary) when is_binary(Binary) ->
    gen_server:start_link(?MODULE, [{binary, Binary}], []).

close(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, close).

get_primitive_arrays(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_primitive_arrays, infinity).

get_primitive_array(Pid, ObjectId) when is_pid(Pid) ->
    gen_server:call(Pid, {get_primitive_array, ObjectId}, infinity).

get_string(Pid, StringId) when is_pid(Pid) ->
    gen_server:call(Pid, {get_string, StringId}).

get_class_dump(Pid, ClassId) when is_pid(Pid) ->
    gen_server:call(Pid, {get_class, ClassId}).

get_instances_for_class(Pid, ClassName) when is_pid(Pid) and is_binary(ClassName) ->
    gen_server:call(Pid, {get_instances_for_class, ClassName}, infinity).

get_bitmaps(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_bitmaps, infinity).

get_instance(Pid, InstanceId) when is_pid(Pid) ->
    gen_server:call(Pid, {get_instance, InstanceId}, infinity).

%% Callbacks

init([{file, Filename}]) ->
    gen_server:cast(self(), {parse_file, Filename}),
    {ok, init_ets(#state{})};
init([{binary, Binary}]) ->
    gen_server:cast(self(), {parse_binary, Binary}),
    {ok, init_ets(#state{})}.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(get_primitive_arrays, _From, State) ->
    {reply, ets:tab2list(State#state.ets_primitive_array), State};
handle_call({get_primitive_array, ObjectId}, _From, State) ->
    {reply, ets_get(State#state.ets_primitive_array, ObjectId), State};
handle_call({get_string, StringId}, _From, State) ->
    Result = case ets:lookup(State#state.ets_strings, StringId) of
        [] -> not_found;
        [#hprof_record_string{data=S}|_] -> S
    end,
    {reply, Result, State};
handle_call({get_class, ClassId}, _From, State) ->
    Result = case ets:lookup(State#state.ets_class_dump, ClassId) of
        [] -> not_found;
        [S|_] -> S
    end,
    {reply, Result, State};
handle_call({get_instances_for_class, ClassName}, _From, State) ->
    Result = get_instances_for_class_impl(State, ClassName),
    {reply, Result, State};
handle_call(get_bitmaps, _From, State) ->
    Result = get_bitmaps_impl(State),
    {reply, Result, State};
handle_call({get_instance, InstanceId}, _From, State) ->
    Result = get_heap_instance(State, InstanceId),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({parse_file, Filename}, State) ->
    State1 = parse_file(State, Filename),
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

parse_file(State, Filename) ->
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
        ets_heap_instance = ets:new(heap_instance, [set, {keypos, 2}, compressed]),
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

get_field(_Id, []) -> not_found;
get_field(Id, [Field=#hprof_instance_field{name_string_id=Id}|_Fields]) -> Field;
get_field(Id, [_|Fields]) -> get_field(Id, Fields).

get_bitmaps_impl(State) ->
    case get_instances_for_class_impl(State, <<"android.graphics.Bitmap">>) of
        {error, Reason} -> {error, Reason};
        {ok, Instances} ->
            % Get the string IDs for the properties we care about
            MWidth = get_id_for_string(State, <<"mWidth">>),
            MHeight = get_id_for_string(State, <<"mHeight">>),
            MBuffer = get_id_for_string(State, <<"mBuffer">>),
            % If any of those strings don't exist, something is wrong
            case lists:any(fun(X) -> X =:= not_found end, [MWidth, MHeight, MBuffer]) of
                true ->
                    {error, string_table_incomplete};
                false ->
                    Bitmaps = [{bitmap,
                      get_field(MWidth, Values),
                      get_field(MHeight, Values),
                      get_field(MBuffer, Values)} ||
                      #hprof_heap_instance{instance_values=Values}
                      <- Instances
                    ],
                    [Bmp || Bmp={bitmap, W, H, B} <- Bitmaps,
                     W =/= not_found,
                     H =/= not_found,
                     B =/= not_found]
            end
    end.


get_id_for_string(State, String) when is_binary(String) ->
    case ets:lookup(State#state.ets_strings_reverse, String) of
        [] -> not_found;
        [{_, Id}] -> Id
    end.

get_heap_instance(State, Id) ->
    case ets:lookup(State#state.ets_heap_instance, Id) of
        [] -> not_found;
        [Instance] -> Instance
    end.

get_instances_for_class_impl(State, ClassName) ->
    % Get the string ID for the class name
    case ets:lookup(State#state.ets_strings_reverse, ClassName) of
        [] -> {error, class_name_not_found};
        [{_, StringId}] ->
            % Now find the class object for this string ID
            case get_class_obj_by_name_id(State, StringId) of
                not_found -> {error, class_not_found};
                #hprof_class_dump{class_object=COD} ->
                    {ok, get_instances_with_class_object_id(State, COD)}
            end
    end.

get_instances_with_class_object_id(State, ClsObjId) ->
    ets:select(
        State#state.ets_heap_instance,
        ets:fun2ms(
          fun(F=#hprof_heap_instance{class_object_id=CID}) when ClsObjId =:= CID -> F end)
     ).

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
    {State1, RecordsBinary} = parse_header(State, Bin),

    % Do our first parsing pass, which handles all the record types except for
    % the instances, which can only be dealt with once we have all the class
    % definitions. The parse_records method will insert non-instance records
    % directly into ETS.
    State2 = parse_records_optimized(State1, RecordsBinary),

    % Now that we have all these instance records, parse them and hoover
    % them all into ETS.
    lists_flat_foreach(
        fun(R=#hprof_heap_instance_raw{}) ->
            ets:insert(
                State2#state.ets_heap_instance,
                parse_instance_record(State1, R)
            )
        end,
        State2#state.raw_instance_records
    ),

    erlang:garbage_collect(),
    io:format("Finished loading~n"),
    State2.

lists_flat_foreach(_Fun, []) -> ok;
lists_flat_foreach(Fun, [Sublist|Rest]) when is_list(Sublist) ->
    lists_flat_foreach(Fun, Sublist),
    lists_flat_foreach(Fun, Rest);
lists_flat_foreach(Fun, [E|Elements]) ->
    Fun(E),
    lists_flat_foreach(Fun, Elements).

parse_instance_record(State, R=#hprof_heap_instance_raw{}) ->
    % Break out the record data
    #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    } = R,

    % Get the ref size for parsing later
    RefSize = State#state.heap_ref_size,
    Values = parse_instance_record_for_class(
        State, RefSize, ClassObjectId, Data, []
    ),
    #hprof_heap_instance{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        instance_values=Values
    }.

parse_instance_record_for_class(_State, _RefSize, 0, _Binary, Acc) ->
    Acc;
parse_instance_record_for_class(State, RefSize, ClassId, Binary, Acc) ->
    % Get the class instance for this object
    ClassObj = hd(ets:lookup(State#state.ets_class_dump, ClassId)),

    % Get the list of instance fields to read data for, and extract them
    ClassInstanceFields = ClassObj#hprof_class_dump.instance_fields,
    SuperClassId = ClassObj#hprof_class_dump.superclass_object,
    {FieldValues, Rest} = extract_instance_id_fields(
        RefSize, ClassInstanceFields, Binary, #{}
    ),
    parse_instance_record_for_class(
        State, RefSize, SuperClassId, Rest, [FieldValues|Acc]
    ).

extract_instance_id_fields(_RefSize, [], Binary, Acc) ->
    {Acc, Binary};
extract_instance_id_fields(RefSize, [Field|Fields], Binary, Acc) ->
    #hprof_instance_field{name_string_id=Name, type=Type} = Field,
    DataSize = primitive_size(RefSize, Type),
    <<PrimitiveBin:DataSize/binary, Rest/binary>> = Binary,
    Value = #hprof_instance_field{
        name_string_id=Name,
        type=Type,
        value=parse_primitive(PrimitiveBin, Type)
    },
    extract_instance_id_fields(
        RefSize, Fields, Rest, maps:put(Name, Value, Acc)
    ).

parse_header(State, Bindata) ->
    % Header has the format:
    % Fixed header (JAVA PROFILE 1.0.3)
    % u32: Pointer size (in bytes)
    % u64: Time this dump was taken (milliseconds)
    <<?HPROF_HEADER_MAGIC,
      HeapRefSize:?UINT32,
      DumpTimeMs:?UINT64,
      Rest/binary >> = Bindata,
    {State#state{
        heap_ref_size = HeapRefSize,
        dump_timestamp_ms = DumpTimeMs
    }, Rest}.

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
    % Loading a class
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
    io:format("Hit new heap segment of size ~p~n", [Size]),
    parse_heap_dump_segments_optimized(State, Binary, Size);
parse_record_optimized(State, Size, <<Binary/binary>>, ?HPROF_TAG_HEAP_DUMP_END) ->
    % Nothing much doing here
    <<_:Size/binary, Rest/binary>> = Binary,
    parse_records_optimized(State, Rest).

parse_heap_dump_segments_optimized(State, <<Binary/binary>>, 0) ->
    io:format("Finished parsing heap segment~n"),
    parse_records_optimized(State, Binary);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_UNKNOWN, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: Unknown root~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_GLOBAL, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root jni global~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_LOCAL, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root jni local~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JAVA_FRAME, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root java frame~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_NATIVE_STACK, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root native stack~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_STICKY_CLASS, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root sticky class~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_THREAD_BLOCK, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root thread block~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_MONITOR_USED, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root monitor used~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_THREAD_OBJECT, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root thread object~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    ets:insert(State#state.ets_roots_thread_object, Root),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_CLASS_DUMP, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: class dump~n"),
    parse_class_dump_segment_optimized(State, Bin, RemainingBytes - 1);
parse_heap_dump_segments_optimized(State, <<?HPROF_INSTANCE_DUMP, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: instance dump~n"),
    % Can't actually parse these until we have the class dump data
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ClassObjectId:RefSize/big-unsigned-integer-unit:8,
      DataSize:?UINT32,
      Rest/binary>> = Bin,
    <<Data:DataSize/binary, Rest1/binary>> = Rest,
    BytesRead = 1 + RefSize + 4 + RefSize + 4 + DataSize,
    Instance = #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    },
    State1 = State#state{
        raw_instance_records=[Instance|State#state.raw_instance_records]
    },
    io:format("Parsed: ~p~n", [Instance]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State1, Rest1, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_OBJECT_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: object array dump~n"),
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
    io:format("Parsed: ~p~n", [Array]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_PRIMITIVE_ARRAY_DUMP, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: primitive array dump~n"),
    RefSize = State#state.heap_ref_size,
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      DataType:?UINT8,
      Rest/binary>> = Bin,
    BytesRead = 1 + RefSize + 9,
    parse_primitive_array_dump(
        State, DataType, ElementCount, ObjectId,
        StackTraceSerial, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_HEAP_DUMP_INFO, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: heap dump info~n"),
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
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_INTERNED_STRING, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root interned string~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_ROOT_FINALIZING, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_finalizing});
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_DEBUGGER, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root debugger~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_ROOT_REFERENCE_CLEANUP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_reference_cleanup});
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_VM_INTERNAL, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root vm internal~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(State, <<?HPROF_ROOT_JNI_MONITOR, Bin/binary>>, RemainingBytes) ->
    io:format("Seg: root jni monitor~n"),
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
    io:format("Parsed: ~p~n", [Root]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - BytesRead]),
    parse_heap_dump_segments_optimized(State, Rest, RemainingBytes - BytesRead);
parse_heap_dump_segments_optimized(_State, <<?HPROF_UNREACHABLE, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_root_unreachable});
parse_heap_dump_segments_optimized(_State, <<?HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, _/binary>>, _RemainingBytes) ->
    throw({obsolete_tag, hprof_primitive_array_nodata_dump}).

parse_primitive_array_dump(State, DataType, ElementCount, ObjectId, StackTraceSerial, <<Binary/binary>>, RemainingBytes) ->
    RefSize = State#state.heap_ref_size,
    ElementSize = primitive_size(RefSize, DataType),
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
    io:format("Parsed: ~p~n", [Array]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - DataSize]),
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - DataSize).

parse_records(State, Binary) when is_binary(Binary) ->
    parse_records(State, Binary, []).
parse_records(_State, <<>>, Acc) ->
    Acc;
parse_records(State, Binary, Acc) ->
    % Each record has the format:
    % u8: Record type
    % u32: Microseconds since header timestamp
    % u32: Size of this record (not including this header)
    % [u8] data
    <<RecordType:?UINT8,
      _Microseconds:?UINT32,
      RecordSize:?UINT32,
      Rest/binary>> = Binary,
    parse_records(State, Rest, Acc, RecordType, RecordSize).

parse_records(State, Rest, Acc, RecordType, RecordSize) ->
    <<RecordData:RecordSize/binary, Rest1/binary>> = Rest,
    Acc1 = case parse_record(
        State,
        State#state.heap_ref_size,
        RecordType, RecordData
    ) of
        % Instance records come as part of the heap dump segments, so they'll
        % be in lists. Filter out the non-instances, and just stick the list on
        % front to be flattened later.
        L when is_list(L) -> [L|Acc];
        _ -> Acc
    end,
    parse_records(State, Rest1, Acc1).

parse_record(State, RefSize, ?HPROF_TAG_STRING, Binary) ->
    % Contains an ordinary utf-8 string
    <<Id:RefSize/big-unsigned-integer-unit:8,
      Data/binary>> = Binary,
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
    });
parse_record(State, RefSize, ?HPROF_TAG_LOAD_CLASS, Binary) ->
    % Loading a class
    % u32: Serial number
    % Ref: Class object ID
    % u32: Stack trace serial number
    % Ref: Class name string ID
    <<Serial:?UINT32,
      ClassObjId:RefSize/big-unsigned-integer-unit:8,
      StackSerial:?UINT32,
      ClassNameId:RefSize/big-unsigned-integer-unit:8
    >> = Binary,
    Record = #hprof_record_load_class{
        serial=Serial,
        class_object_id=ClassObjId,
        stack_trace_serial=StackSerial,
        class_name_string_id=ClassNameId
    },
    ets:insert(State#state.ets_class_load, Record);
parse_record(_State, _RefSize, ?HPROF_TAG_UNLOAD_CLASS, _Binary) ->
    % Loading a class
    % We don't really care about these records
    % u32: Serial number
    % <<Serial:?UINT32
    % >> = Raw#hprof_record_raw.raw_data,
    % Record = #hprof_record_unload_class{
    %     serial=Serial
    % },
    ok;
parse_record(State, RefSize, ?HPROF_TAG_STACK_FRAME, Binary) ->
    % Stack frame
    % Ref: Stack fram ID
    % Ref: Method name string ID
    % Ref: Method signature string ID
    % Ref: Source file name string ID
    % u32: Class serial number
    % u32: Location
    <<FrameId:RefSize/big-unsigned-integer-unit:8,
      MethodNameStringId:RefSize/big-unsigned-integer-unit:8,
      MethodSigStringId:RefSize/big-unsigned-integer-unit:8,
      SourceFileStringId:RefSize/big-unsigned-integer-unit:8,
      ClassSerial:?UINT32,
      Location:?UINT32
    >> = Binary,
    Record = #hprof_record_stack_frame{
        frame_id=FrameId,
        method_name_string_id=MethodNameStringId,
        method_signature_string_id=MethodSigStringId,
        source_file_string_id=SourceFileStringId,
        class_serial=ClassSerial,
        location=Location
    },
    ets:insert(State#state.ets_stack_frame, Record);
parse_record(State, RefSize, ?HPROF_TAG_STACK_TRACE, Binary) ->
    % Stack trace
    % u32: Stack trace serial
    % u32: Thread serial
    % u32: Number of frames
    % [Ref]: Stack fram IDs
    <<Serial:?UINT32,
      ThreadSerial:?UINT32,
      FrameCount:?UINT32,
      FrameIdsBin/binary
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
    ets:insert(State#state.ets_stack_trace, Record);
parse_record(State, RefSize, ?HPROF_TAG_HEAP_DUMP_SEGMENT, Binary) ->
    parse_heap_dump_segments(State, RefSize, Binary);
parse_record(_State, _RefSize, ?HPROF_TAG_HEAP_DUMP_END, _Binary) ->
    % Nothing much doing here
    ok.

parse_heap_dump_segments(State, RefSize, #hprof_record_raw{raw_data=Bin}) ->
    _Segments = parse_heap_dump_segments(State, RefSize, Bin, []).
parse_heap_dump_segments(_State, _RefSize, <<>>, Acc) ->
    Acc;
parse_heap_dump_segments(State, RefSize, Bin, Acc) ->
    {Record, Rest} = parse_heap_dump_segment(State, RefSize, Bin),
    Acc1 = case Record of
        I=#hprof_heap_instance_raw{} -> [I|Acc];
        _ -> Acc
    end,
    parse_heap_dump_segments(State, RefSize, Rest, Acc1).

parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_UNKNOWN, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8, Rest/binary>> = Bin,
    Root = #hprof_heap_root_unknown{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=unknown
    }),
    ets:insert(State#state.ets_roots_unknown, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_JNI_GLOBAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      JniGlobalRefId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_global{
        object_id=ObjectId,
        jni_global_ref_id=JniGlobalRefId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_global
    }),
    ets:insert(State#state.ets_roots_jni_global, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_JNI_LOCAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_local{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_local
    }),
    ets:insert(State#state.ets_roots_jni_local, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_JAVA_FRAME, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_java_frame{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=java_fram
    }),
    ets:insert(State#state.ets_roots_java_frame, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_NATIVE_STACK, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_native_stack{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=native_stack
    }),
    ets:insert(State#state.ets_roots_native_stack, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_STICKY_CLASS, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_sticky_class{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=sticky_class
    }),
    ets:insert(State#state.ets_roots_sticky_class, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_THREAD_BLOCK, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_thread_block{
        object_id=ObjectId,
        thread_serial=ThreadSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=thread_block
    }),
    ets:insert(State#state.ets_roots_thread_block, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_MONITOR_USED, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_monitor_used{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=monitor_used
    }),
    ets:insert(State#state.ets_roots_monitor_used, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_THREAD_OBJECT, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      StackTraceSerial:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_thread_object{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        stack_trace_serial=StackTraceSerial
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=thread_object
    }),
    ets:insert(State#state.ets_roots_thread_object, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_CLASS_DUMP, Bin/binary>>) ->
    {Class, Rest} = parse_class_dump_segment(RefSize, Bin),
    ets:insert(State#state.ets_class_dump, Class),
    {ok, Rest};
parse_heap_dump_segment(_State, RefSize, <<?HPROF_INSTANCE_DUMP, Bin/binary>>) ->
    % Can't actually parse these until we have the class dump data
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ClassObjectId:RefSize/big-unsigned-integer-unit:8,
      DataSize:?UINT32,
      Rest/binary>> = Bin,
    <<Data:DataSize/binary, Rest1/binary>> = Rest,
    Instance = #hprof_heap_instance_raw{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        class_object_id=ClassObjectId,
        data=Data
    },
    {Instance, Rest1};
parse_heap_dump_segment(State, RefSize, <<?HPROF_OBJECT_ARRAY_DUMP, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      ElementClassObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    DataSize = ElementCount * RefSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Rest,
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
    {ok, Rest1};
parse_heap_dump_segment(State, RefSize, <<?HPROF_PRIMITIVE_ARRAY_DUMP, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      StackTraceSerial:?UINT32,
      ElementCount:?UINT32,
      DataType:?UINT8,
      Rest/binary>> = Bin,
    ElementSize = primitive_size(RefSize, DataType),
    DataSize = ElementCount * ElementSize,
    <<ArrayData:DataSize/binary, Rest1/binary>> = Rest,
    Elements = [
        parse_primitive(DataType, Elem) || <<Elem:ElementSize/binary>>
        <= ArrayData
    ],
    Array = #hprof_primitive_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_type=DataType,
        elements=Elements
    },
    ets:insert(State#state.ets_primitive_array, Array),
    {ok, Rest1};
parse_heap_dump_segment(_State, RefSize, <<?HPROF_HEAP_DUMP_INFO, Bin/binary>>) ->
    % Not currently tracking which heap things are in
    <<_HeapType:?UINT32,
      _HeapNameStringId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    % Info = #hprof_heap_dump_info{
    %     heap_type=HeapType,
    %     heap_type_string_id=HeapNameStringId
    % },
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_INTERNED_STRING, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_interned_string{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=interned_string
    }),
    ets:insert(State#state.ets_roots_interned_string, Root),
    {ok, Rest};
parse_heap_dump_segment(_State, _RefSize, <<?HPROF_ROOT_FINALIZING, _/binary>>) ->
    throw({obsolete_tag, hprof_root_finalizing});
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_DEBUGGER, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_debugger{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=debugger
    }),
    ets:insert(State#state.ets_roots_debugger, Root),
    {ok, Rest};
parse_heap_dump_segment(_State, _RefSize, <<?HPROF_ROOT_REFERENCE_CLEANUP, _/binary>>) ->
    throw({obsolete_tag, hprof_root_reference_cleanup});
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_VM_INTERNAL, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_vm_internal{
        object_id=ObjectId
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=vm_internal
    }),
    ets:insert(State#state.ets_roots_vm_internal, Root),
    {ok, Rest};
parse_heap_dump_segment(State, RefSize, <<?HPROF_ROOT_JNI_MONITOR, Bin/binary>>) ->
    <<ObjectId:RefSize/big-unsigned-integer-unit:8,
      ThreadSerial:?UINT32,
      FrameNum:?UINT32,
      Rest/binary>> = Bin,
    Root = #hprof_heap_root_jni_monitor{
        object_id=ObjectId,
        thread_serial=ThreadSerial,
        frame_number=FrameNum
    },
    ets:insert(State#state.ets_roots, #hprof_root{
        object_id=ObjectId, root_type=jni_monitor
    }),
    ets:insert(State#state.ets_roots_jni_monitor, Root),
    {ok, Rest};
parse_heap_dump_segment(_State, _RefSize, <<?HPROF_UNREACHABLE, _/binary>>) ->
    throw({obsolete_tag, hprof_root_unreachable});
parse_heap_dump_segment(_State, _RefSize, <<?HPROF_PRIMITIVE_ARRAY_NODATA_DUMP, _/binary>>) ->
    throw({obsolete_tag, hprof_primitive_array_nodata_dump}).

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

    % Empty constant pool for ART, apparently, but may as well be complete
    % about it.
    {Constants, Rest2, ConstantsBytesRead} = parse_class_dump_constants(
        RefSize, NumConstants, Rest1
    ),

    % Static fields
    <<NumStatics:?UINT16, Rest3/binary>> = Rest2,
    {Statics, Rest4, StaticFieldBytesRead} = parse_class_dump_static_fields(
        RefSize, NumStatics, Rest3
    ),

    % Instance Fields
    <<NumInstances:?UINT16, Rest5/binary>> = Rest4,
    {Instances, Rest6, InstanceFieldBytesRead} = parse_class_dump_instance_fields(
        RefSize, NumInstances, Rest5
    ),

    Class = #hprof_class_dump{
        class_object=ClassObjId,
        stack_trace_serial=StackTraceSerial,
        superclass_object=SuperClassObjId,
        classloader_object=ClassLoaderObjId,
        signer=Signer,
        prot_domain=ProtDomain,
        instance_size=InstanceSize,
        num_constants=NumConstants,
        constants=Constants,
        num_static_fields=NumStatics,
        static_fields=Statics,
        num_instance_fields=NumInstances,
        instance_fields=Instances
    },

    ets:insert(State#state.ets_class_dump, Class),
    TotalBytesRead = (
        HeaderBytesRead + ConstantsBytesRead +
        StaticFieldBytesRead + InstanceFieldBytesRead +
        4 % The UINT16s for number of static/instance fields
    ),

    io:format("Parsed: ~p~n", [Class]),
    io:format("Bytes remaining: ~p~n", [RemainingBytes - TotalBytesRead]),
    parse_heap_dump_segments_optimized(State, Rest6, RemainingBytes - TotalBytesRead).

parse_class_dump_segment(RefSize, Bin) ->
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

    % Empty constant pool for ART, apparently
    {Constants, Rest2} = parse_class_dump_constants(RefSize, NumConstants, Rest1),

    % Static fields
    <<NumStatics:?UINT16, Rest3/binary>> = Rest2,
    {Statics, Rest4} = parse_class_dump_static_fields(RefSize, NumStatics, Rest3),

    % Instance Fields
    <<NumInstances:?UINT16, Rest5/binary>> = Rest4,
    {Instances, Rest6} = parse_class_dump_instance_fields(RefSize, NumInstances, Rest5),

    Class = #hprof_class_dump{
        class_object=ClassObjId,
        stack_trace_serial=StackTraceSerial,
        superclass_object=SuperClassObjId,
        classloader_object=ClassLoaderObjId,
        signer=Signer,
        prot_domain=ProtDomain,
        instance_size=InstanceSize,
        num_constants=NumConstants,
        constants=Constants,
        num_static_fields=NumStatics,
        static_fields=Statics,
        num_instance_fields=NumInstances,
        instance_fields=Instances
    },
    {Class, Rest6}.

parse_class_dump_constants(RefSize, NumConstants, Binary) ->
    parse_class_dump_constants(RefSize, NumConstants, Binary, [], 0).
parse_class_dump_constants(_RefSize, 0, Binary, Acc, BytesConsumed) ->
    {lists:reverse(Acc), Binary, BytesConsumed};
parse_class_dump_constants(RefSize, NumConstants, Binary, Acc, BytesConsumed) ->
    <<ConstantPoolIndex:?UINT16,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(RefSize, Type),
    <<FieldDataBin:FieldSize/binary, Rest1/binary>> = Rest,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_constant_field{
        constant_pool_index=ConstantPoolIndex,
        type=Type,
        data=FieldData
    },
    parse_class_dump_constants(
        RefSize, NumConstants-1, Rest1, [Field|Acc],
        BytesConsumed + 2 + 1 + FieldSize
    ).

parse_class_dump_static_fields(RefSize, NumStatics, Binary) ->
    parse_class_dump_static_fields(RefSize, NumStatics, Binary, [], 0).
parse_class_dump_static_fields(_RefSize, 0, Binary, Acc, BytesConsumed) ->
    {lists:reverse(Acc), Binary, BytesConsumed};
parse_class_dump_static_fields(RefSize, NumStatics, Binary, Acc, BytesConsumed) ->
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    FieldSize = primitive_size(RefSize, Type),
    <<FieldDataBin:FieldSize/binary, Rest1/binary>> = Rest,
    FieldData = parse_primitive(FieldDataBin, Type),
    Field = #hprof_static_field{
        name_string_id=FieldNameStringId,
        type=Type,
        data=FieldData
    },
    parse_class_dump_static_fields(
        RefSize, NumStatics-1, Rest1, [Field|Acc],
        BytesConsumed + RefSize + 1 + FieldSize
    ).

parse_class_dump_instance_fields(RefSize, NumConstants, Binary) ->
    parse_class_dump_instance_fields(RefSize, NumConstants, Binary, [], 0).
parse_class_dump_instance_fields(_RefSize, 0, Binary, Acc, BytesConsumed) ->
     {lists:reverse(Acc), Binary, BytesConsumed};
parse_class_dump_instance_fields(RefSize, NumConstants, Binary, Acc, BytesConsumed) ->
    <<FieldNameStringId:RefSize/big-unsigned-integer-unit:8,
      Type:?UINT8,
      Rest/binary
    >> = Binary,
    Field = #hprof_instance_field{
        name_string_id=FieldNameStringId,
        type=Type
    },
    parse_class_dump_instance_fields(
        RefSize, NumConstants-1, Rest, [Field|Acc],
        BytesConsumed + RefSize + 1
    ).
