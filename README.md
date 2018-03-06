# Android HPROF Parser

Based off the hprof dumping implementation for ART, visible
[here](https://android.googlesource.com/platform/art/+/oreo-release/runtime/hprof/hprof.cc)

Under the [scripts](scripts/) folder is a collection of tools that are useful but
too special-case to be part of the library itself.

## Usage

Create a parser context for either a file or loaded binary

```erlang
1> {ok, Parser} = hprof_parser:parse_file('large.hprof').
% or
1> {ok, Parser} = hprof_parser:parse_binary(MyHprofData).
```

You can then ask the parser for instances of a particular class, or have it
stream all instances to you. Note that parsing *all* instances can use a
significant amount of heap memory! Most instances tend to be very small, and
will be copied to the EHeap instead of being stored as subbinaries of the
refc hprof data binary.

```erlang
1> {ok, Ref} = hprof_parser:get_all_instances(Parser).
{ok,#Ref<0.1785039001.3435397121.175788>}
1> {ok, Ref} = hprof_parser:get_instances_for_class(Parser, <<"android.graphics.Bitmap">>).
{ok,#Ref<0.1785039001.3435397121.175788>}
```

Parsed instances will be sent back to the calling process's message box.
There is a utility method, `hprof:await/2`, that can be used to
consume instances for a given request.

```erlang
1> hprof:await(Ref, fun(Instance) -> io:format("Instance: ~p~n", [Instance]) end).
Instance: {hprof_heap_instance,324587328,undefined,0,1900582544,
              #{<<"mBuffer">> =>
                    {hprof_instance_field,4204792,<<"mBuffer">>,object,
                        3640193024},
                <<"mDensity">> =>
                    {hprof_instance_field,4203498,<<"mDensity">>,int,320},
                <<"mFinalizer">> =>
                    {hprof_instance_field,4203500,<<"mFinalizer">>,object,
                        324667104},
                <<"mHeight">> =>
                    {hprof_instance_field,4203706,<<"mHeight">>,int,76},
                [...]
Instance: [...]
ok
1>
```

There also exists `hprof:await_acc`, which is identical to `hprof:await` but
with an accumulator value between calls.

```erlang
{ok, Parser} = hprof_parser:parse_file('large.hprof').
{ok, Ref} = hprof_parser:get_all_instances(Parser).
{ok, ClassCount} = hprof:await_acc(
    Ref,
    #{},
    fun(Acc, #hprof_heap_instance{class_name=Name}) ->
        maps:update_with(
            Name,
            fun(C) -> C+1 end,
            1,
            Acc
        )
    end
).
{ok,#{<<"android.animation.StateListAnimator$Tuple">> => 12,
      <<"java.lang.Long">> => 648,
      <<"android.support.design.widget.CoordinatorLayout">> => 2,
      <<"com.google.android.gms.tasks.Tas"...>> => 1,
      <<"android."...>> => 1,<<"org."...>> => 1,<<...>> => 2,...}}
```

### Bitmaps

For the special case of `android.graphics.Bitmap`, a regular heap offender, the
methods in `hprof_bitmap` can be used to generate a PNG file from the in-memory
contents of the bitmap. E.g, to save a png for each bitmap present in the dump:

```erlang
{ok, Parser} = hprof_parser:parse_file('large.hprof').
{ok, Ref} = hprof_parser:get_instances_for_class(Parser, <<"android.graphics.Bitmap">>).
SaveFun = fun(Instance) ->
    file:write_file(
        integer_to_list(os:system_time()) ++ ".png",
        element(2, hprof_bitmap:make_png(Parser, Instance))
    )
end.
hprof:await(Ref, SaveFun).
```

### Reference Graphs

In some cases, it may not be clear what code is responsible for a large object.
To help with this, the `hprof_dot` module contains methods for generating a
reference graph (in GraphViz format) for a given array ID. If we were to want to
know which classes reference the largest array in the heap, we could do it
like so:

```erlang
{ok, Parser} = hprof_parser:parse_file("large.hprof").
% Get all of the byte arrays
{ok, ArrRef} = hprof_parser:get_primitive_arrays_of_type(Parser, byte).
% Only keep the largest one
{ok, LargestArray} = hprof:await_acc(
    ArrRef,
    #hprof_primitive_array{elements = <<>>},
    fun(A1=#hprof_primitive_array{elements=E1}, A2=#hprof_primitive_array{elements=E2}) ->
        case byte_size(E1) > byte_size(E2) of true -> A1; false -> A2 end
    end
).
% Now build a reference graph, depth 5, based on that array's ID
GraphChunks = hprof_dot:reference_graph_for_array(
    Parser, LargestArray#hprof_primitive_array.object_id, 5
).
% Write that out
file:write_file("out.dot", GraphChunks).
```

`dot` files can be converted to images using a variation on the command:

    dot -Tpng out.dot > out.png

![Reference Graph](/reference_graph.png?raw=true "Reference Graph")

Note that this module is somewhat slow - it prioritizes minimizing heap usage
over runtime, and as such re-parses the instance fields of a dump multiple times
instead of caching them all in memory. Some of the heaps tested have had
staggering numbers of small classes, which have enough overhead to bloat the
heaps significantly (e.g, one dump had 7,713,323 linked list nodes, each
pointing to two other small classes. Loading all instances into memory consumed
~6GiB of heap).

In addition, be conservative with how many levels of references to traverse -
while the module will happily produce the graph definition, GraphViz quickly
chokes on digraphs with more than 4 or 5 levels.


# License

Copyright 2018 Ross Schlaikjer

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
