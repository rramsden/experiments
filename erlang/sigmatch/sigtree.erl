%%%-------------------------------------------------------------------
%%% @author Richard Ramsden
%%% @doc
%%% SIGMATCH is a generic filtering technique that can be plugged in
%%% as a pre-processing step for any existing multi-pattern matching
%%% system.
%%% @end
%%%-------------------------------------------------------------------
-module(sigtree).
-export([new/1, match/2]).

-define(ALPHABET, 256).
-define(TABLE_SIZE, (?ALPHABET * ?ALPHABET)).

% tuning parameters for sigtree
-define(B, 2).
-define(BETA, 4).
-define(BGRAM, ?B + ?BETA).

new(Signatures) ->
    TabId = ets:new(sigtree, [ordered_set, {read_concurrency, true}]),
    Cover = cover(Signatures),
    build_index(TabId, Signatures, Cover),
    {sigtree, TabId}.

match(String, {sigtree, TabId}) ->
    walk_text(TabId, String).

walk_text(_, Str) when length(Str) =< ?B ->
    nomatch;
walk_text(TabId, [_|T] = String) ->
    [Chars, Rest] = first(?B, String),
    TrieIndex = list_to_int(Chars),
    
    case ets:lookup(TabId, TrieIndex) of
        [{TrieIndex, Bloom}] ->
            case bloom:is_element(Rest, Bloom) of
                true ->
                    true;
                false ->
                    walk_text(TabId, T)
            end;
        [] ->
            walk_text(TabId, T)
    end.
    
build_index(_, [], _) ->
    ok;
build_index(TabId, [Sig|T], Cover) ->
    walk_bgrams(TabId, Sig, Cover),
    build_index(TabId, T, Cover).

walk_bgrams(_, _, []) ->
    ok;
walk_bgrams(TabId, Sig, [BGram | T]) ->
    case representative_substring(list_to_binary(Sig), BGram) of
        {match, Substring} ->
            add_index(TabId, Sig, Substring);
        nomatch ->
            walk_bgrams(TabId, Sig, T)
    end.

add_index(TabId, _Sig, Substring) ->
    [Chars, Rest] = first(?B, binary_to_list(Substring)),
    Index = list_to_int(Chars),

    case ets:lookup(TabId, Index) of
        [] ->
            B = bloom:bloom(4000),
            bloom:add_element(Rest, B),
            ets:insert(TabId, {Index, B});
        [{Index, B1}] ->
            B2 = bloom:add_element(Rest, B1),
            ets:insert(TabId, {Index, B2})
    end.

%%%
%% @private
%% @doc
%% Constructs an effecient B-Gram cover given a signature set
%% @end
cover(Signatures) ->
    lists:reverse(cover(Signatures, [])).
cover(Signatures0, Acc) ->
    TabId = cover_setup(),

    build(TabId, Signatures0),
    BGram = highest_occurrence(TabId),

    cover_cleanup(TabId),

    case BGram of
        <<1>> ->
            Acc;
        _ ->
            cover(prune(Signatures0, BGram), [BGram| Acc])
    end.

cover_setup() ->
    TabId = ets:new(frequency_table, [ordered_set, public]),
    [ets:insert(TabId, {I, 0}) || I <- lists:seq(1, ?TABLE_SIZE)],
    TabId.
cover_cleanup(TabId) ->
    true = ets:delete(TabId).

%%%
%% @private
%% @doc
%% Given a signature set prunes every occurence of the representative
%% substring in respect to a BGram
%% @end
prune(SigSet, BGram) ->
    prune(SigSet, [], BGram).
prune([], Pruned, _) ->
    Pruned;
prune([Sig|T], Pruned, BGram) ->
    case representative_substring(list_to_binary(Sig), BGram) of
        {match, _} ->
            % discard the signature if it is a representative
            % substring. These are (b + Beta) bytes long
            prune(T, Pruned, BGram);
        nomatch ->
            prune(T, [Sig|Pruned], BGram)
    end.

%%%
%% @private
%% @doc
%% Representative substrings are strings constructed using
%% a B-Gram followed by BETA bytes. The first B-bytes are used as an 
%% index into a trie structure whereas the last BETA bytes are hashed into a bloomfilter
%% @end
representative_substring(String, BGram) ->
    case binary:match(String, BGram) of
        {Start, Size} ->
            <<_:Start/binary, BGram:Size/binary, Rest/binary>> = String,
            case size(Rest) >= ?BETA of
                true ->
                    {match, <<BGram/binary, Rest/binary>>};
                false ->
                    nomatch
            end;
        nomatch ->
            nomatch
    end.
    
%%%
%% @private
%% @doc
%% This function will search through our frequency table we constructed
%% in build/2 and extract the b-gram which has the highest frequency
%% @end
highest_occurrence(TabId) ->
    {BGram, _} = lists:foldl(
        fun({Index, Size}, {IndexOld, Max}) ->
                case Size > Max of
                    true ->
                        {Index, Size};
                    false ->
                        {IndexOld, Max}
                end
        end,
        {-1, -1},
        ets:tab2list(TabId)
    ),
    int_to_bin(BGram).

build(TabId, Signatures) ->
    build(1, TabId, Signatures).
build(_Pos, _TabId, []) ->
    ok;
build(Pos, TabId, [Sig | Rest]) ->
    left_to_right_pass(Pos, Sig, TabId),
    build(Pos + 1, TabId, Rest).

left_to_right_pass(_Pos, Str, _TabId) when length(Str) < ?BGRAM ->
    ok;
left_to_right_pass(Pos, Str, TabId) ->
    [Chars, Rest] = first(?B, Str),
    Index = list_to_int(Chars),
    ets:update_counter(TabId, Index, 1),
    left_to_right_pass(Pos, Rest, TabId).

first(N, List) ->
    first(N, List, []).
first(0, Tail, Acc) ->
    [Acc, Tail];
first(Count, [H|T], Acc) ->
    first(Count - 1, T, Acc ++ [H]).

list_to_int(List) ->
    binary:decode_unsigned(list_to_binary(List)).
int_to_bin(Bin) ->
    binary:encode_unsigned(Bin).
