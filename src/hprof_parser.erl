-module(hprof_parser).
-behavior(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("hprof/include/records.hrl").

% Public API
-export([
    parse_file/1,
    parse_binary/1,
    close/1,
    % Get the reference size for the dump
    reference_size/1,
    % Fetch a single string from the string table
    get_string/2,
    % Get the name for a class by id
    get_name_for_class_id/2,
    % Fetch all heap instances. Response is streamed.
    get_all_instances/1,
    % Fetch instances matching a specific class name. Streamed.
    get_instances_for_class/2,
    % Fetch all primitive arrays of a given type
    get_primitive_arrays_of_type/2,
    % Getch all object arrays
    get_object_arrays/1,
    % Fetch a primitive array, by ID
    get_primitive_array/2,
    % Get the reference root for an object
    get_object_root/2,
    % Get all dumped class info
    get_class_dumps/1
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
    ets_class_instance_parser :: reference(),
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

-spec parse_file(string()) -> {ok, pid()}.
parse_file(Filename) ->
    gen_server:start_link(?MODULE, [{file, Filename}], []).

-spec parse_binary(binary()) -> {ok, pid()}.
parse_binary(Binary) when is_binary(Binary) ->
    gen_server:start_link(?MODULE, [{binary, Binary}], []).

-spec close(pid()) -> ok.
close(Pid) when is_pid(Pid) ->
    call(Pid, close).

-spec reference_size(pid()) -> 8 | 4.
reference_size(Pid) when is_pid(Pid) ->
    call(Pid, get_reference_size).

-spec get_name_for_class_id(pid(), pos_integer()) -> binary() | not_found.
get_name_for_class_id(Pid, ClassObjectId) when is_pid(Pid) ->
    call(Pid, {get_name_for_class_id, ClassObjectId}).

-spec get_primitive_array(pid(), pos_integer()) -> #hprof_primitive_array{} | not_found.
get_primitive_array(Pid, ObjectId) when is_pid(Pid) ->
    call(Pid, {get_primitive_array, ObjectId}).

-spec get_primitive_arrays_of_type(pid(), atom()) -> list(#hprof_primitive_array{}).
get_primitive_arrays_of_type(Pid, Type) when is_pid(Pid) ->
    call(Pid, {get_primitive_arrays_for_type, self(), Type}).

-spec get_string(pid(), pos_integer()) -> binary() | not_found.
get_string(Pid, StringId) when is_pid(Pid) ->
    call(Pid, {get_string, StringId}).

-spec get_all_instances(pid()) -> {ok, reference()}.
get_all_instances(Pid) when is_pid(Pid) ->
    call(Pid, {get_all_instances, self()}).

-spec get_instances_for_class(pid(), binary()) -> {ok, reference()} | {error, any()}.
get_instances_for_class(Pid, ClassName) when is_pid(Pid) and is_binary(ClassName) ->
    call(Pid, {get_instances_for_class, ClassName, self()}).

-spec get_object_arrays(pid()) -> {ok, reference()}.
get_object_arrays(Pid) when is_pid(Pid) ->
    call(Pid, {get_object_arrays, self()}).

-spec get_object_root(pid(), pos_integer()) -> {ok, atom()}.
get_object_root(Pid, ObjectId) when is_pid(Pid) ->
    call(Pid, {get_object_root, ObjectId}).

-spec get_class_dumps(pid()) -> {ok, reference()}.
get_class_dumps(Pid) when is_pid(Pid) ->
    call(Pid, {get_class_dumps, self()}).

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
handle_call(get_reference_size, _From, State) ->
    {reply, State#state.heap_ref_size, State};
handle_call({get_name_for_class_id, ClassObjectId}, _From, State) ->
    Resp = case ets_get(State#state.ets_class_name_by_id, ClassObjectId) of
        {_, V} -> V;
        Other -> Other
    end,
    {reply, Resp, State};
handle_call({get_primitive_array, ObjectId}, _From, State) ->
    {reply, ets_get(State#state.ets_primitive_array, ObjectId), State};
handle_call({get_primitive_arrays_for_type, Caller, Type}, _From, State) ->
    {reply, get_primitive_arrays_for_type_impl(State, Caller, Type), State};
handle_call({get_string, StringId}, _From, State) ->
    Return = case ets_get(State#state.ets_strings, StringId) of
        #hprof_record_string{data=V} -> V;
        _ -> not_found
    end,
    {reply, Return, State};
handle_call({get_all_instances, Caller}, _From, State) ->
    {reply, stream_instances(State, Caller), State};
handle_call({get_instances_for_class, ClassName, Caller}, _From, State) ->
    {reply, stream_instances(State, Caller, ClassName), State};
handle_call({get_object_arrays, Caller}, _From, State) ->
    {reply, get_object_arrays_impl(State, Caller), State};
handle_call({get_class_dumps, Caller}, _From, State) ->
    {reply, get_class_dumps_impl(State, Caller), State};
handle_call({get_object_root, ObjId}, _From, State) ->
    Ret = case ets_get(State#state.ets_roots, ObjId) of
        #hprof_root{root_type=Type} -> Type;
        _ -> not_found
    end,
    {reply, Ret, State};
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
    stream_instances_filter(State, Caller, fun(_ObjId, _ClassId) -> true end).

stream_instances(State, Caller, ClassName) when is_binary(ClassName) ->
    % Get the string ID for the class name
    case get_id_for_string(State, ClassName) of
        not_found -> {error, class_name_not_found};
        StringId ->
            case get_class_obj_by_name_id(State, StringId) of
                not_found -> {error, class_obj_not_found};
                ClassObj ->
                    AcceptFun = fun(_ObjectId, ClassId) ->
                        ClassId =:= ClassObj#hprof_class_dump.class_id
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
        Caller, Ref, AcceptFun,
        State#state.hprof_data,
        State#state.ets_class_instance_parser,
        State#state.ets_class_name_by_id
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
        ets_class_instance_parser = ets:new(class_instance_parser, [set]),
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

get_primitive_arrays_for_type_impl(State, Caller, Type) ->
    % Convert the atom to a type ordinal
    Ref = make_ref(),
    PrimOrdinal = hprof:primitive_ordinal(Type),
    spawn(fun() ->
        ets:foldl(
            fun(Element=#hprof_primitive_array{element_type=EType}, _Acc) ->
                case PrimOrdinal =:= EType of
                    true ->
                        Caller ! {hprof_parser, Ref, Element};
                    false ->
                        ok
                end
            end,
            ok,
            State#state.ets_primitive_array
        ),
        Caller ! {hprof_parser, Ref, ok}
    end),
    {ok, Ref}.

get_class_dumps_impl(State, Caller) ->
    Ref = make_ref(),
    spawn(fun() ->
        ets:foldl(
            fun(Element, _Acc) ->
                Caller ! {hprof_parser, Ref, Element}
            end,
            ok,
            State#state.ets_class_dump
        ),
        Caller ! {hprof_parser, Ref, ok}
    end),
    {ok, Ref}.

get_object_arrays_impl(State, Caller) ->
    Ref = make_ref(),
    spawn(fun() ->
        ets:foldl(
            fun(Element, _Acc) ->
                Caller ! {hprof_parser, Ref, Element}
            end,
            ok,
            State#state.ets_object_array
        ),
        Caller ! {hprof_parser, Ref, ok}
    end),
    {ok, Ref}.

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

initialize_class_name_by_id(State=#state{}) ->
    ets:foldl(
        fun(#hprof_record_load_class{class_object_id=Cid, class_name_string_id=StringId}, _) ->
            StringVal = case ets:lookup(State#state.ets_strings, StringId) of
                [#hprof_record_string{data=V}] -> V;
                _ -> not_found
            end,
            ets:insert(State#state.ets_class_name_by_id, {Cid, StringVal})
        end,
        ok,
        State#state.ets_class_load
    ).

create_parsers_for_instances(State) ->
    ets:foldl(
        fun(#hprof_class_dump{class_id=ClassId}, _) ->
            ets:insert(
                State#state.ets_class_instance_parser,
                {ClassId, create_parser_for_class(State, ClassId)}
            )
        end,
        ok,
        State#state.ets_class_dump
    ).

create_parser_for_class(State, ClassId) ->
    create_parser_for_class(State, ClassId, []).

create_parser_for_class(_State, 0, FunAcc) ->
    ParserFun = lists:foldl(
        fun (Function, Accumulator) -> Function(Accumulator) end,
        fun (<<>>, Acc) -> Acc end,
        FunAcc
    ),
    fun(Data) -> ParserFun(Data, #{}) end;
create_parser_for_class(State, ClassId, FunAcc) ->
    ClassObj = hd(ets:lookup(State#state.ets_class_dump, ClassId)),
    ClassInstanceFields = ClassObj#hprof_class_dump.instance_fields,
    FieldParsers = create_parser_for_fields(State, ClassInstanceFields, FunAcc),
    SuperClassId = ClassObj#hprof_class_dump.superclass_object,
    create_parser_for_class(State, SuperClassId, FieldParsers).

create_parser_for_fields(_State, [], FunAcc) ->
    FunAcc;
create_parser_for_fields(State, [Field|Fields], FunAcc) ->
    create_parser_for_fields(
        State,
        Fields,
        [create_parser_for_field(
            State,
            Field#hprof_instance_field.type,
            Field#hprof_instance_field.name_string_id
        )|FunAcc]
    ).

create_parser_for_field(State, Type, NameId) ->
    NameString= case ets_get(State#state.ets_strings, NameId) of
        #hprof_record_string{data=V} -> V;
        _ -> not_found
    end,
    DataSize = hprof:primitive_size(State#state.heap_ref_size, Type),
    OrdType = hprof:primitive_ordinal(Type),
    fun (Cont) ->
        fun(<<PrimitiveBin:DataSize/binary, Rest/binary>>, Acc) ->
            Field = #hprof_instance_field{
                name_string_id=NameId,
                name=NameString,
                type=Type,
                value=hprof:parse_primitive(PrimitiveBin, OrdType)
            },
            Cont(Rest, maps:put(NameString, Field, Acc))
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
    create_parsers_for_instances(State),
    initialize_class_name_by_id(State),
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
    ElementSize = hprof:primitive_size(RefSize, DataType),
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
    Elements = case DataType of
        ?HPROF_BASIC_BYTE -> ArrayData;
        ?HPROF_BASIC_CHAR -> ArrayData;
        _ ->
            [hprof:parse_primitive(Elem, DataType) || <<Elem:ElementSize/binary>>
             <= ArrayData]
    end,
    Array = #hprof_primitive_array{
        object_id=ObjectId,
        stack_trace_serial=StackTraceSerial,
        element_type=DataType,
        elements=Elements
    },
    ets:insert(State#state.ets_primitive_array, Array),
    parse_heap_dump_segments_optimized(State, Rest1, RemainingBytes - DataSize).

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
        class_id=ClassObjId,
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
    FieldSize = hprof:primitive_size(RefSize, Type),
    parse_class_dump_constant(
        State, Rest, Class, NumConstants, Acc,
        RemainingBytes, ConstantPoolIndex, Type, FieldSize
    ).

parse_class_dump_constant(State, <<Binary/binary>>, Class, NumConstants, Acc,
                          RemainingBytes, ConstantPoolIndex, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = hprof:parse_primitive(FieldDataBin, Type),
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
    FieldSize = hprof:primitive_size(RefSize, Type),
    parse_class_dump_static(
        State, Rest, NumStatics, Acc, Class, RemainingBytes - (RefSize + 1),
        FieldNameStringId, Type, FieldSize
    ).

parse_class_dump_static(State, <<Binary/binary>>, NumStatics, Acc, Class, RemainingBytes, FieldNameStringId, Type, FieldSize) ->
    <<FieldDataBin:FieldSize/binary, Rest/binary>> = Binary,
    FieldData = hprof:parse_primitive(FieldDataBin, Type),
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
        type=hprof:primitive_name(Type)
    },
    parse_class_dump_instance_fields(
        State, Rest, NumFields-1, [Field|Acc],
        Class, RemainingBytes - (RefSize + 1)
    ).

parse_class_dump_segment_finalize(State, <<Bin/binary>>, Class, RemainingBytes) ->
    ets:insert(State#state.ets_class_dump, Class),
    parse_heap_dump_segments_optimized(State, Bin, RemainingBytes).
