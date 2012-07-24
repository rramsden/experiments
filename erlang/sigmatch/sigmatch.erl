%%%-------------------------------------------------------------------
%%% @author Richard Ramsden
%%% @doc
%%% SIGMATCH is a generic filtering technique that can be plugged in
%%% as a pre-processing step for any existing multi-pattern matching
%%% system. The sigmatch filter sits in front of an existing multi-pattern
%%% matching system as seen below:
%%%
%%%             +--------------+               +---------------+
%%%      Text   |              |     maybe     |               |
%%%  +--------->|   SigMatch   |+------------->|     Dict      |
%%%             |              |+-------+      |               |
%%%             +--------------+        |      +---------------+
%%%                    +                |          |       |    
%%%                    | nomatch        | match    | match | nomatch
%%%                    |                |          |       |
%%%                    v                v          v       v
%%%
%%% In this implementation we use a Dict which stores a list of possible
%%% matches when a `maybe` occurs.
%%% @end
%%%-------------------------------------------------------------------
-module(sigmatch).
-export([new/1, match/2]).

-define(l2b(X), list_to_binary(X)).
-define(b2l(X), binary_to_list(X)).
-define(t2b(X), term_to_binary(X)).
-define(b2t(X), binary_to_term(X)).

-record(signature, {
    pattern, % re
    chomped 
}).

-record(leaf, {
    bloom, 
    matches = []
}).

-record(tables, {
    trie = dict:new() :: dict:dict(any(), #leaf{}),
    dict = dict:new() :: dict:dict(any(), [string()])
}).

% tuning parameters for sigtree
-define(B, 2). % # of characters for trie index
-define(BETA, 4). % # of characters to hash into bloomfilter

% misc
-define(ALPHABET, 256).
-define(BGRAM_TABLE_SIZE, (?ALPHABET * ?ALPHABET)).

%%%
%% @doc
%% new/1
%%
%% Generates a sigtree for a list of patterns
%% @end
new(Patterns) ->
    Signatures = lists:map(fun(P) -> 
        #signature{pattern=P, chomped=regex_chomp(P)}
    end, Patterns),
    Cover = cover(Signatures),
    Tables0 = build_index(#tables{}, Signatures, Cover),
    Tables1 = compress_tree(Tables0),
    {sigtree, Tables1}.

compress_tree(#tables{dict=Dict0, trie=Trie0} = Tables) ->
    Dict1 = dict:map(fun(_, Patterns) -> compress(Patterns) end, Dict0),
    Trie1 = dict:map(
            % bloomfilters are fat, compress them down
            fun(_, #leaf{bloom=B0} = Leaf) -> 
                    B1 = compress(B0),
                    Leaf#leaf{bloom=B1}
            end, Trie0
    ),
    Tables#tables{dict=Dict1, trie=Trie1}.

%%%
%% @doc
%% match/2
%%
%% Since were trying to match patterns against a text string
%% we need to walk the text character by character until we hit
%% a match. Once we hit a match we check the leafs bloomfilter to
%% see if its in the set. If it is use a lookup dict for possible matches
%% @end
match(Text, {sigtree, Tables}) ->
    walk_text(Tables, Text, Text).

walk_text(_, _, Substr) when length(Substr) =< ?B -> nomatch;
walk_text(#tables{trie=Trie, dict=Dict} = Tables, Text, [_|T] = Substr) ->
    [B, Beta] = split(Substr, ?B, ?BETA),
    TrieIndex = B,

    case dict:find(TrieIndex, Trie) of
        {ok, #leaf{matches=Matches, bloom=C}} ->
            Bloom = uncompress(C),
            InMatches = re_match_any(Text, Matches),
            DictIndex = string:substr(Substr, 1, ?B + ?BETA),
            case (InMatches orelse (in_bloom(Beta, Bloom) andalso in_dict(Dict, DictIndex, Text))) of
                true -> match;
                false -> walk_text(Tables, Text, T)
            end;
        _ ->
            walk_text(Tables, Text, T)
    end.

in_bloom(_, undefined) ->
    false;
in_bloom(Value, BF) ->
    bloom:is_element(Value, BF).

in_dict(Dict, DictIndex, Text) ->
    case dict:find(DictIndex, Dict) of
        {ok, C} ->
            Matches = uncompress(C),
            re_match_any(Text, Matches);
        _ ->
            false
    end.

re_match_any(Text, Matches) ->
    lists:any(fun(Pattern) -> re:run(Text, Pattern) =/= nomatch end, Matches).

compress(Leaf) ->
    zlib:compress(?t2b(Leaf)).
uncompress(Leaf) ->
    ?b2t(zlib:uncompress(Leaf)).

build_index(Tables, [], _) -> Tables;
build_index(Tables0, [Sig|T], Cover) ->
    Tables1 = case walk_bgrams(Tables0, Sig, Cover) of
        {match, Substring} -> add_index(Tables0, Sig, ?b2l(Substring));
        nomatch -> add_index(Tables0, Sig, Sig#signature.chomped)
    end,
    build_index(Tables1, T, Cover). 

walk_bgrams(_, _, []) -> nomatch;
walk_bgrams(Tables, #signature{chomped=Chomped} = Sig, [BGram | T]) ->
    case representative_substring(?l2b(Chomped), ?l2b(BGram)) of
        {match, Substring} ->
            {match, Substring};
        nomatch ->
            walk_bgrams(Tables, Sig, T)
    end.

add_index(#tables{trie=Trie0, dict=Dict0}, Signature, Substring) ->
    Trie1 = add_trie_index(Trie0, Signature, Substring),
    Dict1 = add_dict_index(Dict0, Signature, Substring),
    #tables{trie=Trie1, dict=Dict1}.

%%% store all patterns for future lookup
add_dict_index(Dict, _, Substring) when length(Substring) < (?B + ?BETA) ->
    Dict;
add_dict_index(Dict, Sig, Substring) ->
    Index = string:substr(Substring, 1, ?B + ?BETA),

    case dict:find(Index, Dict) of
        error ->
            dict:store(Index, [Sig#signature.pattern], Dict);
        {ok, Matches} ->
            dict:store(Index, [Sig#signature.pattern|Matches], Dict)
    end.

%%% store bloomfilters for runtime checks
add_trie_index(Trie, Signature, Substring) ->
    [TrieIndex, Rest] = split(Substring, ?B, ?BETA),
    add_trie_index(Trie, TrieIndex, Signature, Rest).

add_trie_index(Trie, TrieIndex, Sig, Rest) when length(Rest) < ?BETA ->
    case dict:find(TrieIndex, Trie) of
        error ->
            dict:store(TrieIndex, #leaf{matches=[Sig#signature.pattern]}, Trie);
        {ok, #leaf{matches=Matches} = Leaf0} ->
            Leaf1 = Leaf0#leaf{matches=[Sig#signature.pattern | Matches]},
            dict:store(TrieIndex, Leaf1, Trie)
    end;
add_trie_index(Trie, TrieIndex, _, Rest) ->
    BFNew = case dict:find(TrieIndex, Trie) of
        error ->
            BF = bloom:sbf(26700),
            #leaf{bloom = bloom:add_element(Rest, BF)};
        {ok, #leaf{bloom=BF0} = Leaf} ->
            BF1 = case BF0 of undefined -> bloom:sbf(26700); _ -> BF0 end,
            Leaf#leaf{bloom = bloom:add_element(Rest, BF1)}
    end,
    dict:store(TrieIndex, BFNew, Trie).

%%%
%% @private
%% @doc
%% Constructs an effecient B-Gram cover given a signature set
%% @end
cover(Signatures) ->
    cover(Signatures, []).
cover(Signatures0, Acc) ->
    TabId = ets:new(frequency_table, [set]),
    build_cover(TabId, Signatures0),
    BGram = highest_occurrence(TabId),
    ets:delete(TabId),

    case BGram of
        undefined -> Acc;
        _ -> cover(prune(Signatures0, BGram), [BGram| Acc])
    end.

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
    case representative_substring(?l2b(Sig#signature.chomped), ?l2b(BGram)) of
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
    {BGram, Size} = lists:foldl(
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
    case Size of
        -1 -> undefined;
        _ -> BGram
    end.

build_cover(TabId, Signatures) ->
    build_cover(1, TabId, Signatures).
build_cover(_Pos, _TabId, []) ->
    ok;
build_cover(Pos, TabId, [Sig | Rest]) ->
    extract_covers_from_string(Sig#signature.chomped, TabId),
    build_cover(Pos + 1, TabId, Rest).

extract_covers_from_string(Str, _TabId) when length(Str) < (?B + ?BETA) ->
    ok;
extract_covers_from_string([_|T] = Str, TabId) ->
    [Index, _] = split(Str, ?B, ?BETA),
    case catch ets:update_counter(TabId, Index, 1) of
        X when is_number(X) -> ok;
        _ -> ets:insert(TabId, {Index, 1})
    end,
    extract_covers_from_string(T, TabId).

regex_chomp(Pattern0) ->
    Pattern1 = re:replace(Pattern0, "[\\^\\$\\|\\?\\*\\+\\(\\)]+", "", [global, {return, list}]),
    Pattern2 = re:replace(Pattern1, "\\\\[a-zA-Z]", "", [global, {return, list}]),
    re:replace(Pattern2, "\\\\.", ".", [global, {return, list}]).

split(Str, _, _) when length(Str) =< ?B ->
    [Str, []];
split(Str, Start, End) ->
    P1 = string:substr(Str, 1, Start),
    P2 = string:substr(Str, Start + 1, End),
    [P1, P2].


-ifndef(TEST).
-include_lib("eunit/include/eunit.hrl").

small_pattern_test() ->
    Patterns = [
        "\\bcat\\b",
        "cow",
        "dog",
        "i ",
        "xl van"
    ],
    S = sigmatch:new(Patterns),
    ?assertEqual(match, sigmatch:match("there is a cat in the bag", S)),
    ?assertEqual(nomatch, sigmatch:match("catz", S)),
    ?assertEqual(match, sigmatch:match("cow", S)),
    ?assertEqual(match, sigmatch:match(" dog", S)),
    ?assertEqual(match, sigmatch:match("dog ", S)),
    ?assertEqual(match, sigmatch:match(" dog ", S)),
    ?assertEqual(match, sigmatch:match("fat cow", S)),
    ?assertEqual(match, sigmatch:match("foo i bar", S)),
    ?assertEqual(match, sigmatch:match("xl vanz", S)),
    ?assertEqual(nomatch, sigmatch:match("foobar", S)),
    ?assertEqual(nomatch, sigmatch:match("the lazy foo jumped over the moon", S)).

regex_chomp_test() ->
    Patterns = [
        "^exact match$",
        "\\bboundary match\\b",
        "^255\\.255\\.255\\.*$"
    ],
    Expected = [
        "exact match",
        "boundary match",
        "255.255.255."
    ],
    ?assertEqual(lists:map(fun(P) -> regex_chomp(P) end, Patterns), Expected).

big_pattern_test() ->
    Patterns = [
        "will this big pattern match :D",
        "will this big pattern match ;D",
        "will this big pattern match :O",
        "this clearly should pass the test"
    ],
    S = sigmatch:new(Patterns),
    ?assertEqual(match, sigmatch:match("will this big pattern match ;D", S)),
    ?assertEqual(nomatch, sigmatch:match("will this big pattern match ;X", S)),
    ?assertEqual(match, sigmatch:match("I think this clearly should pass the test", S)).

-endif.
