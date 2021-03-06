-module(hprof_bitmap).
-include_lib("hprof/include/records.hrl").

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
                #hprof_primitive_array{elements=Bytes} ->
                    % Make a best guess as to which colourspace this image is.
                    % If there are 4 bytes per pixel, odds are it's ARGB_8888
                    % If there are 2 BPP, then it could be either ARGB_4444 or
                    % RGB_565, so we need to make an educated guess
                    BytesPerPixel = byte_size(Bytes) div (
                        Width#hprof_instance_field.value *
                        Height#hprof_instance_field.value
                    ),
                    ImageMode = case BytesPerPixel of
                        4 ->
                            ?BITMAP_MODE_ARGB_8888;
                        2 ->
                            % Need a tie breaker
                            case is_data_likely_argb4444(Bytes) of
                                true -> ?BITMAP_MODE_ARGB_4444;
                                false -> ?BITMAP_MODE_RGB_565
                            end;
                        1 ->
                            ?BITMAP_MODE_A_8;
                        _ ->
                            {error, {unknown_bit_depth,
                                     Width#hprof_instance_field.value,
                                     Height#hprof_instance_field.value,
                                     byte_size(Bytes)
                            }}
                    end,
                    case ImageMode of
                        {error, Reason} -> {error, Reason};
                        _ ->
                            make_png(
                                ImageMode,
                                Width#hprof_instance_field.value,
                                Height#hprof_instance_field.value,
                                Bytes
                            )
                    end
            end
    end.

% Make a best-guess at whether this image is in ARGB_4444 form or RGB_565 form.
% As a heuristic, check how many pixels would be fully transparent if the image
% were to be alpha. If more than some percentage would be transparent, this
% image probably is.
is_data_likely_argb4444(<<Bin/binary>>) ->
    is_data_likely_argb4444(Bin, 0, 0).
is_data_likely_argb4444(<<>>, Transparent, NonTransparent) ->
    % If more than one in ten pixels would be fully transparent, odds are good
    % it's ARGB
    Transparent >= (NonTransparent div 10);
is_data_likely_argb4444(<<_B:4, A:4, _R:4, _G:4, Rest/binary>>, Transparent, NonTransparent) ->
    case A of
        0 -> is_data_likely_argb4444(Rest, Transparent + 1, NonTransparent);
        _ -> is_data_likely_argb4444(Rest, Transparent, NonTransparent + 1)
    end.

make_png(?BITMAP_MODE_A_8, Width, Height, Data) ->
    Rgb8888Data = <<
        <<0:8, 0:8, 0:8, A:8
        >> || <<A:8>> <= Data
    >>,
    make_png(?BITMAP_MODE_ARGB_8888, Width, Height, Rgb8888Data);
make_png(?BITMAP_MODE_ARGB_4444, Width, Height, Data) ->
    % Ordering of these fields is a bit weird lookin because
    % of endianness differences
    Rgb8888Data = <<
        <<
          ((R bsl 4) bor R):8,
          ((G bsl 4) bor G):8,
          ((B bsl 4) bor B):8,
          ((A bsl 4) bor A):8
        >> || <<B:4, A:4, R:4, G:4>> <= Data
    >>,
    make_png(?BITMAP_MODE_ARGB_8888, Width, Height, Rgb8888Data);
make_png(?BITMAP_MODE_RGB_565, Width, Height, Data) ->
    % Since the data crosses byte boundaries, we need to shuffle
    % the green channel a bit to get it out correctly
    Rgb8888Data = <<
        <<
          (((R * 527) + 23) bsr 6):8,
          (((((GHigh bsl 3) bor GLow) * 259) + 33) bsr 6):8,
          (((B * 527) + 23) bsr 6):8,
          255:8
        >> || <<GLow:3, B:5, R:5, GHigh:3>> <= Data
    >>,
    make_png(?BITMAP_MODE_ARGB_8888, Width, Height, Rgb8888Data);
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
