# Android HPROF Parser

Based off the hprof dumping implementation for ART, visible
[here](https://android.googlesource.com/platform/art/+/oreo-release/runtime/hprof/hprof.cc)

## Usage

Create a parser context for either a file or loaded binary

    1> {ok, Parser} = hprof_parser:parse_file('large.hprof').
    % or
    1> {ok, Parser} = hprof_parser:parse_binary(MyHprofData).

You can then ask the parser for instances of a particular class, or have it
stream all instances to you. Note that parsing *all* instances can use a
significant amount of heap memory! Most instances tend to be very small, and
will be copied to the EHeap instead of being stored as subbinaries of the
refc hprof data binary.

    1> {ok, Ref} = hprof_parser:get_all_instances(Parser).
    {ok,#Ref<0.1785039001.3435397121.175788>}
    1> {ok, Ref} = hprof_parser:get_instances_for_class(Parser, <<"android.graphics.Bitmap">>).
    {ok,#Ref<0.1785039001.3435397121.175788>}

Parsed instances will be sent back to the calling process's message box.
There is a utility method, `hprof:await/2`, that can be used to
consume instances for a given request.

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

For the special case of `android.graphics.Bitmap`, a regular heap offender, the
methods in `hprof_bitmap` can be used to generate a PNG file from the in-memory
contents of the bitmap. E.g, to save a png for each bitmap present in the dump:

    {ok, Parser} = hprof_parser:parse_file('large.hprof').
    {ok, Ref} = hprof_parser:get_instances_for_class(Parser, <<"android.graphics.Bitmap">>).
    SaveFun = fun(Instance) ->
        file:write_file(
            integer_to_list(os:system_time()) ++ ".png",
            element(2, hprof_bitmap:make_png(Parser, Instance))
        )
    end.
    hprof:await(Ref, SaveFun).

There also exists `hprof:await_acc`, which is identical to `hprof:await` but
with an accumulator value between calls.

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
