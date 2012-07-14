-module(util).
-export([readlines/1]).

readlines(FileName) ->
    {ok, Device} = file:open(FileName, [read]),
    get_all_lines(Device, []).

get_all_lines(Device, Accum) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device), Accum;
        Line_ -> 
            Line = string:to_lower(string:strip(Line_, right, $\n)),
            get_all_lines(Device, [Line|Accum])
    end.
