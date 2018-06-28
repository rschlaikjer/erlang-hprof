-module(main).
-export([main/1]).

-include_lib("hprof/include/records.hrl").

main([DumpFile|TargetInstanceIds]) ->
    io:format("Loading heap dump file ~s~n", [DumpFile]),
    {ok, Parser} = hprof_parser:parse_file(DumpFile),

    % Precalc the reference maps.
    {ObjectReferenceMap, StaticObjectMap} = precalc_ets_tables(Parser),

    % Spawn a process to start searching the heap graph
    Self = self(),
    lists:foreach(
        fun(TargetInstanceId) ->
            spawn_link(fun() ->
                find_reference_chains(
                    Self, ObjectReferenceMap, StaticObjectMap,
                    list_to_integer(TargetInstanceId),
                    sets:new()
                )
             end)
        end,
        TargetInstanceIds
    ),

    % Enter the receive + print loop
    io:format("Waiting for reference chains...~n"),
    recv_print_loop(Parser, [list_to_integer(T) || T <- TargetInstanceIds]).

recv_print_loop(Parser, []) ->
    hprof_parser:close(Parser),
    ok;
recv_print_loop(Parser, Targets) ->
    receive
        {chain, _From, RefChain} ->
            print_ref_chain(Parser, RefChain),
            recv_print_loop(Parser, Targets);
        {done, Target} ->
            recv_print_loop(Parser, [T || T <- Targets, T =/= Target])
    end.

% In order to reduce the search space and cut down on false positives, ignore
% any reference chains that would go through these instances,
% Finalizers are just VM noise, and the Weak* objects aren't going to be the
% ones causing problems.
should_skip_instance(#hprof_heap_instance{class_name = <<"java.lang.ref.FinalizerReference">>}) -> true;
should_skip_instance(#hprof_heap_instance{class_name = <<"java.lang.ref.WeakReference">>}) -> true;
should_skip_instance(#hprof_heap_instance{class_name = <<"java.util.WeakHashMap$Entry">>}) -> true;
should_skip_instance(#hprof_heap_instance{class_name = <<"sun.misc.Cleaner">>}) -> true;
should_skip_instance(_) -> false.

% Utility method for checking if a field references a nonnull object
field_references_object(#hprof_instance_field{type=T, value=V}) ->
    (T =:= object) and (V =/= 0);
field_references_object(#hprof_static_field{type=T, data=D}) ->
    (T =:= ?HPROF_BASIC_OBJECT) and (D =/= 0).

% Calculate two ETS tables to make analysis easier:
% - A bag of object ID -> object ID of instance that reference that object ID
% - A set of object IDs that are referenced by a static class variable
precalc_ets_tables(Parser) ->
    % ETS of Object ID -> Referring Object IDs
    ObjectIdsByReferringObjectId = ets:new(
        object_ids, [bag, public, {read_concurrency, true}]
    ),

    % Normal set of statically referenced object IDs
    StaticallyReferencedObjects = ets:new(
        static_objects, [set, public, {read_concurrency, true}]
    ),

    %% Populate the referenced by referent table
    % Instances
    io:format("Populating reference mapping table...~n"),
    TabLoadStart = os:system_time(),

    io:format(" - Instances...~n"),
    {ok, InstancesRef} = hprof_parser:get_all_instances(Parser),
    ok = hprof:await(
        InstancesRef,
        fun(I=#hprof_heap_instance{object_id=Oid, instance_values=Values}) ->
            case should_skip_instance(I) of
                true -> ok;
                false -> maps:fold(
                    fun(_Key, F=#hprof_instance_field{value=V}, _Acc) ->
                        case field_references_object(F) of
                            true -> ets:insert(ObjectIdsByReferringObjectId, {V, Oid});
                            false -> ok
                        end
                    end,
                    ok,
                    Values
                )
            end
        end
    ),

    % Object arrays
    io:format(" - Object arrays...~n"),
    {ok, ObjectArrayRef} = hprof_parser:get_object_arrays(Parser),
    ok = hprof:await(
        ObjectArrayRef,
        fun(#hprof_object_array{object_id=Arrid, elements=Elements}) ->
            lists:foreach(
                fun(Element) ->
                    ets:insert(ObjectIdsByReferringObjectId, {Element, Arrid})
                end,
                Elements
            )
        end
    ),

    % Class dumps
    io:format(" - Class dumps...~n"),
    {ok, ClassDumpRef} = hprof_parser:get_class_dumps(Parser),
    ok = hprof:await(
        ClassDumpRef,
        fun(#hprof_class_dump{class_id=ClsId, static_fields=Statics}) ->
            lists:foreach(
                fun(F=#hprof_static_field{data=D}) ->
                    case field_references_object(F) of
                        true ->
                            ets:insert(ObjectIdsByReferringObjectId, {D, ClsId}),
                            ets:insert(StaticallyReferencedObjects, {D});
                        false -> ok
                    end
                end,
                Statics
            )
        end
    ),

    % Print a summary
    TabInfo = ets:info(ObjectIdsByReferringObjectId),
    TabLoadDuration = (os:system_time() - TabLoadStart) div 1000000000,
    io:format("Loaded ~p object mappings in ~p seconds~n", [
        proplists:get_value(size, TabInfo),
        TabLoadDuration
    ]),

    {ObjectIdsByReferringObjectId, StaticallyReferencedObjects}.

ets_contains(Ets, Key) ->
    case ets:lookup(Ets, Key) of
        [] -> false;
        _ -> true
    end.

print_ref_chain(Parser, RefChain) ->
    % Get the set of instance IDs to match
    InstanceIdSet = sets:from_list(RefChain),

    % Stream instances to get the ones we care about
    {ok, InstanceRef} = hprof_parser:get_all_instances(Parser),
    {ok, InstancesMap} = hprof:await_acc(
        InstanceRef,
        #{},
        fun(Acc, I=#hprof_heap_instance{object_id=Id}) ->
            case sets:is_element(Id, InstanceIdSet) of
                true -> maps:put(Id, I, Acc);
                false -> Acc
            end
        end
    ),

    % Also pull the list of object arrays
    {ok, ObjectArrayRef} = hprof_parser:get_object_arrays(Parser),
    {ok, InstancesMap1} = hprof:await_acc(
        ObjectArrayRef,
        InstancesMap,
        fun(Acc, A=#hprof_object_array{object_id=ArrId}) ->
            case sets:is_element(ArrId, InstanceIdSet) of
                true -> maps:put(ArrId, A, Acc);
                false -> Acc
            end
        end
    ),

    % Figure out which class holds the static reference
    {ok, ClassDumpRef1} = hprof_parser:get_class_dumps(Parser),
    {ok, {StaticRootClass, StaticRootVar}} = hprof:await_acc(
        ClassDumpRef1,
        not_found,
        fun(Acc, C=#hprof_class_dump{static_fields=Fields}) ->
            MatchedVar = lists:foldl(
                fun(F=#hprof_static_field{type=T, data=D}, Acc1) ->
                    case (T =:= ?HPROF_BASIC_OBJECT) and (sets:is_element(D, InstanceIdSet)) of
                        true -> {C, F};
                        false -> Acc1
                    end
                end,
                not_found,
                Fields
            ),
            case MatchedVar of
                not_found -> Acc;
                {C, F} -> {C, F}
            end
        end
    ),

    io:format("Found statically referenced chain: ~n"),
    RootClassName = hprof_parser:get_name_for_class_id(Parser, StaticRootClass#hprof_class_dump.class_id),
    StaticVarName = hprof_parser:get_string(Parser, StaticRootVar#hprof_static_field.name_string_id),
    io:format(" ~s -static ~s-> ~p~n", [RootClassName, StaticVarName, StaticRootVar#hprof_static_field.data]),
    lists:foreach(
        fun(Id) ->
            Instance = maps:get(Id, InstancesMap1, not_found),
            case Instance of
                #hprof_heap_instance{class_name=Name} ->
                    case next_for_instance(Parser, InstanceIdSet, Instance) of
                        not_found -> ok; % End of the chain
                        {VariableName, NextInstanceId} ->
                            io:format(" ~s (~p) -~s-> ~p~n", [Name, Id, VariableName, NextInstanceId])
                    end;
                #hprof_class_dump{class_id=ClsId} ->
                    ClassName = hprof_parser:get_name_for_class_id(Parser, ClsId),
                    {VariableName, NextInstanceId} = next_for_instance(Parser, InstanceIdSet, Instance),
                    io:format(" ~s (~p) -~s-> ~p~n", [ClassName, ClsId, VariableName, NextInstanceId]);
                #hprof_object_array{object_id=ArrId, element_class_object_id=ElementClassId} ->
                    NextInstanceId = next_for_instance(Parser, InstanceIdSet, Instance),
                    ArrayClassName = hprof_parser:get_name_for_class_id(Parser, ElementClassId),
                    io:format(" ~s array (~p) -> ~p~n", [ArrayClassName, ArrId, NextInstanceId]);
                not_found ->
                    io:format(" ??? Unknown object (~p)~n", [Id])
            end
        end,
        RefChain
    ),
    ok.

% Method for figuring out which object is next, and which variable points
% to it.
next_for_instance(_Parser, InstanceIdSet, #hprof_heap_instance{instance_values=Fields}) ->
    maps:fold(
        fun(_, #hprof_instance_field{name=N, type=T, value=D}, Acc) ->
            case (T =:= object) and (sets:is_element(D, InstanceIdSet)) of
                true -> {N, D};
                false -> Acc
            end
        end,
        not_found,
        Fields
    );
next_for_instance(Parser, InstanceIdSet, #hprof_class_dump{static_fields=Fields}) ->
    lists:foldl(
        fun(#hprof_static_field{name_string_id=Nid, type=T, data=D}, Acc) ->
            case (T =:= ?HPROF_BASIC_OBJECT) and (sets:is_element(D, InstanceIdSet)) of
                true ->
                    N = hprof_parser:get_string(Parser, Nid),
                    {N, D};
                false -> Acc
            end
        end,
        not_found,
        Fields
    );
next_for_instance(_Parser, InstanceIdSet, #hprof_object_array{elements=Elements}) ->
    lists:foldl(
        fun(Element, Acc) ->
            case sets:is_element(Element, InstanceIdSet) of
                false -> Acc;
                true -> Element
            end
        end,
        not_found,
        Elements
    ).

find_reference_chains(Caller, ReferrersByReferent, StaticObjects, TargetId, PreviousRoots) ->
    find_reference_chains(Caller, ReferrersByReferent, StaticObjects, TargetId, PreviousRoots, []),
    Caller ! {done, TargetId}.

find_reference_chains(Caller, ReferrersByReferent, StaticObjects, TargetId, PreviousRoots, RefChain) ->
    % Get all objects that refer to our target
    Referers = [Referrer || {_, Referrer} <- ets:lookup(ReferrersByReferent, TargetId)],

    % For each referer, check if we've traversed them before,
    % and if we haven't then check if they're statically referenced.
    % If not, recurse down that rabbit hole.
    % If they are statically referenced, then punt the reference chain
    % over to the printer.
    lists:foreach(
        fun(Referer) ->
            % Check if we already saw this object. This prevents getting stuck
            % if there's a reference cycle.
            case sets:is_element(Referer, PreviousRoots) of
                true -> ok;
                false ->
                    % Check if this object has a static root
                    case ets_contains(StaticObjects, Referer) of
                        false ->
                            % If not, dig deeper
                            find_reference_chains(
                                Caller,
                                ReferrersByReferent, StaticObjects, Referer,
                                sets:add_element(TargetId, PreviousRoots),
                                [TargetId|RefChain]
                            );
                        true ->
                            % Found a chain with a static root, send it to
                            % the printer
                            Caller ! {chain, self(), [Referer|RefChain]}
                    end
            end
        end,
        Referers
    ).
