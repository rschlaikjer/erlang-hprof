-module(hprof_bitmap).
-include("include/records.hrl").

-export([
    make_png/2,
    make_png/4
]).

-define(BITMAP_MODE_A_8, 1).
-define(BITMAP_MODE_RGB_565, 3).
-define(BITMAP_MODE_ARGB_4444, 4).
-define(BITMAP_MODE_ARGB_8888, 5).
-define(BITMAP_MODE_RGBA_F16, 6).

-define(PNG_HEADER, 137, 80, 78, 71, 13, 10, 26, 10).
-define(PNG_HEADER_SEGMENT, <<"IHDR">>).
-define(PNG_DATA_SEGMENT, <<"IDAT">>).
-define(PNG_END, <<"IEND">>).
-define(PNG_ARGB, 6).
-define(PNG_RGB, 2).

make_png(Parser, #hprof_heap_instance{instance_values=V}) when is_pid(Parser) ->
    Width = maps:get(<<"mWidth">>, V, not_found),
    Height = maps:get(<<"mHeight">>, V, not_found),
    Buffer = maps:get(<<"mBuffer">>, V, not_found),
    case lists:any(fun(A) -> A =:= not_found end, [Width, Height, Buffer]) of
        true ->
            {error, bad_bitmap};
        false ->
            case hprof_parser:get_primitive_array(
                Parser, Buffer#hprof_instance_field.value
            ) of
                not_found -> {error, missing_primitive_array};
                #hprof_primitive_array{elements=E} ->
                    Bytes = << <<X>> || X <- E>>,
                    make_png(
                        ?BITMAP_MODE_ARGB_8888,
                        Width#hprof_instance_field.value,
                        Height#hprof_instance_field.value,
                        Bytes
                    )
            end
    end.

make_png(Mode, Width, Height, Data) ->
    Header = make_header(Mode, Width, Height),
    PixelData = encode_pixel_data(Mode, Width, Data),
    Footer = crc_chunk(?PNG_END, <<>>),
    PngBinary = <<
        ?PNG_HEADER,  % Fixed PNG header
        Header/binary,
        PixelData/binary,
        Footer/binary>>,
    {ok, PngBinary}.

encode_pixel_data(?BITMAP_MODE_ARGB_8888, Width, Data) ->
    Zlib = zlib:open(),
    ok = zlib:deflateInit(Zlib),
    RowSize = Width * 4,
    Rows = [
        [0, D] || <<D:RowSize/binary>> <= Data
    ],
    Compressed = zlib:deflate(Zlib, Rows, finish),
    ok = zlib:deflateEnd(Zlib),
    ok = zlib:close(Zlib),
    << <<X/binary>> || X <- [
        crc_chunk(?PNG_DATA_SEGMENT, S) || S <- lists:flatten(Compressed)
    ]>>.

rgb_565_to_888(<<R:5, G:6, B:5>>) ->
    R1 = ((R * 527) + 23) bsr 6,
    G1 = ((G * 259) + 33) bsr 6,
    B1 = ((B * 527) + 23) bsr 6,
    <<R1:8, G1:8, B1:8>>.

make_header(Mode, Width, Height) ->
    ColorType = case has_alpha(Mode) of
        true -> ?PNG_ARGB;
        false -> ?PNG_RGB
    end,
    BitDepth = case Mode of
        ?BITMAP_MODE_ARGB_4444 -> 4;
        _ -> 8
    end,
    HeaderData = <<
      Width:32, Height:32,
      BitDepth:8,  % Bit depth
      ColorType:8,  % Colour type
      0:8,  % Basic compression
      0:8,  % No filter method
      0:8   % No interlace method
    >>,
    crc_chunk(?PNG_HEADER_SEGMENT, HeaderData).

has_alpha(?BITMAP_MODE_A_8) -> true;
has_alpha(?BITMAP_MODE_RGB_565) -> false;
has_alpha(?BITMAP_MODE_ARGB_4444) -> true;
has_alpha(?BITMAP_MODE_ARGB_8888) -> true;
has_alpha(?BITMAP_MODE_RGBA_F16) -> true.

crc_chunk(Type, Data) ->
    DataSize = byte_size(Data),
    Crc = erlang:crc32(<<Type/binary, Data/binary>>),
    <<DataSize:32, Type/binary, Data/binary, Crc:32>>.
