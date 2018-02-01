-module(hprof_dot).

-include("include/records.hrl").

-export([
    reference_graph_for_array/3
]).

reference_graph_for_array(Parser, TargetId, MaxDepth) ->
    % Fetch the primitive array in question
    case hprof_parser:get_primitive_array(Parser, TargetId) of
        not_found -> {error, not_found};
        Array ->
            % Start writing the digraph with the array data
            Chunks1 = handle_primitive_arrays(
                [Array],
                [<<"digraph heap {\n">>]
            ),

            % The only ID we start off trying to resolve is that of the array
            IdsToResolve = sets:from_list([Array#hprof_primitive_array.object_id]),

            % Repeatedly resolve instances that refer to the IDs we care about,
            % until we reach the max depth
            Chunks2 = resolve_ids(
                Parser,
                IdsToResolve, IdsToResolve,
                Chunks1, MaxDepth
            ),

            % Append the closing bracket for the digraph and reverse the
            % accumulator
            lists:reverse([<<"}">>|Chunks2])
    end.

resolve_ids(_Parser, _Ids, _EmittedIds, Chunks, 0) ->
    % Max depth has been reached.
    Chunks;
resolve_ids(Parser, Ids, EmittedIds, Chunks, Iters) ->
    % Start streaming all the heap instances
    % This is slower than parsing all of them once and just iterating over that,
    % but there is a large memory tradeoff. Some heaps take up multiple
    % gigabytes when fully parsed thanks to large numbers of small instances
    % with high overhead.
    {ok, InstanceRef} = hprof_parser:get_all_instances(Parser),

    % Await the parsed instances.
    % For each instance that comes in, we want to update our two sets of IDs,
    % the Ids set (IDs we are currently looking for parents of) and EmittedIds
    % (the IDs of objects we have already recorded to the graph). Whenever we
    % get an instance that points at an Id in the Ids set, we emit a chunk for
    % the instance and add it's Id to both the Ids and EmittedIds sets.
    % The ids set is fresh for each 'generation', the EmittedIds set is
    % persistent across generations to allow for deduping.
    {ok, {Ids1, Emitted1, Chunks1}} = hprof:await_acc(
        InstanceRef,
        {sets:new(), EmittedIds, Chunks},
        fun({I, E, C}, Inst) ->
            update_with_id(Ids, {I, E, C}, Inst)
        end
    ),
    case sets:size(Ids1) of
        0 -> Chunks1;
        _ ->
            resolve_ids(Parser, Ids1, Emitted1, Chunks1, Iters - 1)
    end.

update_with_id(TargetIds, {I, E, C}, Inst=#hprof_heap_instance{object_id=Oid, instance_values=Vals}) ->
    case sets:is_element(Oid, E) of
        true -> {I, E, C};  % This instance is already worked into the graph
        false ->
            % If this instance points to one of our Ids, we need to emit it,
            % and add it's id to our emitted list + new search list.
            Changed = update_chunks_with_instance_fields(Oid, TargetIds, C, Vals),
            case Changed =:= C of
                true ->  {I, E, C}; % No change
                false ->
                    % Create a descriptor chunk for this instance with a human-
                    % readable label
                    ObjAttrs = create_chunk_for_instance(Inst),

                    % Add the ID of this instance to the search set for next
                    % generation and the emitted set
                    {sets:add_element(Oid, I),
                     sets:add_element(Oid, E),
                     [ObjAttrs|Changed]}
            end
    end.

create_chunk_for_instance(Inst) ->
    % Create a label with a human readable name for this instance
    ClassName = case Inst#hprof_heap_instance.class_name of
        B when is_binary(B) -> B;
        _ -> <<"unknown">>
    end,
    ObjIdBin = integer_to_binary(Inst#hprof_heap_instance.object_id),
    ObectName = <<ClassName/binary, " (", ObjIdBin/binary, ")">>,
    ObjId = <<"obj_", ObjIdBin/binary>>,
    <<ObjId/binary, " [label=\"", ObectName/binary,"\"];\n">>.

update_chunks_with_instance_fields(Oid, TargetIds, Chunks, Vals) ->
    % For each instance field of an instance, check if it points to an object
    % and if that object is one of the target objects.
    % If it is, accumulate a mapping chunk onto the chunk list.
    % Returns the updated chunk list.
    maps:fold(
        fun(K, #hprof_instance_field{type=T, value=Target}, MapAcc) ->
            case (T =:= object) and sets:is_element(Target, TargetIds) of
                true ->
                    % This object points to one of the things we care
                    % about, we should emit it
                    ReferentId = <<"obj_", (integer_to_binary(Target))/binary>>,
                    Ref = <<"obj_", (integer_to_binary(Oid))/binary, " -> ", ReferentId/binary,
                            " [taillabel=\"", K/binary, "\"];\n">>,
                    [Ref|MapAcc];
                false ->
                    % This instance doesn't seem useful
                    MapAcc
            end
        end,
        Chunks,
        Vals
    ).

% For a list of object arrays, accumulate chunks for those arrays both
% describing them in human readable terms and linking them to the referent
% objects.
handle_object_arrays(_Parser, [], Acc) ->
    Acc;
handle_object_arrays(Parser, [Arr|Arrays], Acc) ->
    ObjectId = Arr#hprof_object_array.object_id,
    Elements = Arr#hprof_object_array.elements,
    ElementClassId = Arr#hprof_object_array.element_class_object_id,
    ElementTypeName = case hprof_parser:get_name_for_class_id(Parser, ElementClassId) of
        B when is_binary(B) -> B;
        _ -> <<"Object">>
    end,
    ArrayId = <<"obj_", (integer_to_binary(ObjectId))/binary>>,
    ArrayLabel = <<ArrayId/binary, " [label=\"", ElementTypeName/binary,
                   " array of size ", (integer_to_binary(length(Elements)))/binary,
                   " (id ", (integer_to_binary(ObjectId))/binary, ")\"];\n">>,
    Acc2 = lists:foldl(
        fun(ElementObjId, ListAcc) ->
            case ElementObjId of 0 -> ListAcc;
                _ ->
                    [<<ArrayId/binary, " -> obj_",
                     (integer_to_binary(ElementObjId))/binary, ";\n">>|ListAcc]
            end
        end,
        [ArrayLabel|Acc],
        Elements
    ),
    handle_object_arrays(Parser, Arrays, Acc2).

% For a list of primitive arrays, emit a chunk defining their human readable
% name + size.
handle_primitive_arrays([], Acc) ->
    Acc;
handle_primitive_arrays([Arr|Arrays], Acc) ->
    ObjectId = Arr#hprof_primitive_array.object_id,
    ObjectIdBin = integer_to_binary(ObjectId),
    Type = Arr#hprof_primitive_array.element_type,
    Elements = Arr#hprof_primitive_array.elements,
    Size = case Elements of
        B when is_binary(B) -> byte_size(B);
        L when is_list(L) -> length(Elements)
    end,
    ArrId = <<"obj_", ObjectIdBin/binary>>,
    ArrName = <<(atom_to_binary(hprof:primitive_name(Type), utf8))/binary, " array of size ",
                (integer_to_binary(Size))/binary, " (id ", ObjectIdBin/binary, ")">>,
    handle_primitive_arrays(
        Arrays,
        [<<ArrId/binary, " [label=\"", ArrName/binary,
          "\"; scale=", (integer_to_binary(Size))/binary, "; color=\"#ff0000\"];\n">>|Acc]
    ).
