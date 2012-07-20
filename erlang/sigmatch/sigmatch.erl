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
%%%  +--------->|   SigMatch   |+------------->|    ETS Dict   |
%%%             |              |+-------+      |               |
%%%             +--------------+        |      +---------------+
%%%                    +                |          |       |    
%%%                    | nomatch        | match    | match | nomatch
%%%                    |                |          |       |
%%%                    v                v          v       v
%%%
%%% Sigmatch returns either maybe, match, or nomatch depending on
%%% the input.
%%% @end
%%%-------------------------------------------------------------------
-module(sigmatch).
-export([new/1, match/2]).

-define(l2b(X), list_to_binary(X)).
-define(b2l(X), binary_to_list(X)).

% leaf nodes sit at the bottom of the truncated
% trie described in the SigMatch whitepaper. They
% will either contain a bloomfilter or a list of possible
% matches or both.
-record(leaf_node, {
    bloom,
    matches = []
}).

-record(tables, {
    trie,
    dict
}).

% tuning parameters for sigtree
-define(B, 2). % # of characters for trie index
-define(BETA, 4). % # of characters to hash into bloomfilter

% misc
-define(ALPHABET, 256).
-define(BGRAM_TABLE_SIZE, (?ALPHABET * ?ALPHABET)).
-define(default(X, Fun), case X of undefined -> Fun(); X -> X end).

%%%
%% @doc
%% new/1
%%
%% Generates a sigtree for a list of patterns
%% @end
new(Signatures) ->
    Trie = dict:new(),
    Dict = dict:new(),
    Tables0 = #tables{trie=Trie, dict=Dict},
    Cover = lists:reverse(cover(Signatures)),
    Tables1 = build_index(Tables0, Signatures, Cover),
    {sigtree, Tables1}.

%%%
%% @doc
%% match/2
%%
%% Since were trying to match patterns against a text string
%% we need to walk the text character by characters until we hit
%% a match in our ETS ordered set.
%% @end
match(Text, {sigtree, Tables}) ->
    walk_text(Tables, Text, Text).

walk_text(_, _, Substr) when length(Substr) =< ?B -> nomatch;
walk_text(#tables{trie=Trie, dict=Dict} = Tables, Text, [_|T] = Substr) ->
    [B, Beta] = split(Substr, ?B, ?BETA),
    TrieIndex = B,
    DictIndex = string:substr(Substr, 1, ?B + ?BETA),

    case dict:find(TrieIndex, Trie) of
        {ok, Leaf} ->
            case check_leaf(Dict, DictIndex, Leaf, Text, Beta) of
                true -> match;
                false -> walk_text(Tables, Text, T)
            end;
        error ->
            walk_text(Tables, Text, T)
    end.

check_leaf(Dict, DictIndex, Leaf, Text, Beta) ->
    Matches = Leaf#leaf_node.matches,
    BF = Leaf#leaf_node.bloom,
    InMatches = lists:any(fun ([]) -> true; % the index matched the string
                              (SubStr) -> is_prefix(SubStr, Beta)
                          end, Matches),
    InMatches orelse (in_bloom(Beta, BF) andalso in_dict(Dict, DictIndex, Text)).

in_bloom(_, undefined) -> false;
in_bloom(E, BF) -> bloom:is_element(E, BF).

in_dict(Dict, DictIndex, Text) ->
    case dict:find(DictIndex, Dict) of
        error ->
            false;
        {ok, Matches} ->
            lists:any(fun(Pattern) -> re:run(Text, Pattern) =/= nomatch end, Matches)
    end.

is_prefix(SubStr, RS) ->
    string:str(RS, SubStr) == 1.

build_index(Tables, [], _) -> Tables;
build_index(Tables0, [Sig|T], Cover) ->
    Tables1 = case walk_bgrams(Tables0, Sig, Cover) of
        {match, Substring} -> add_index(Tables0, Sig, ?b2l(Substring));
        nomatch -> add_index(Tables0, Sig, Sig)
    end,
    build_index(Tables1, T, Cover). 

walk_bgrams(_, _, []) -> nomatch;
walk_bgrams(Tables, Sig, [BGram | T]) ->
    case representative_substring(?l2b(Sig), ?l2b(BGram)) of
        {match, Substring} ->
            {match, Substring};
        nomatch ->
            walk_bgrams(Tables, Sig, T)
    end.

%%%
%% @private
%% @doc
%% We construct the Trie mentioned in the SigMatch whitepaper
%% by using an ordered ETS Table. The first ?B characters are 
%% stored as an index into the ETS table while the following ?BETA
%% characters are hashed into a bloom filter or appended to a linked list.
%% If the signature is shorter than ?B + ?BETA characters we store possible
%% matches in a linked list.
%% @end
add_index(#tables{trie=Trie0, dict=Dict0}, Signature, Substring) ->
    Trie1 = add_trie_index(Trie0, Substring),
    Dict1 = add_dict_index(Dict0, Signature, Substring),
    #tables{trie=Trie1, dict=Dict1}.

add_dict_index(Dict, Sig, Substring) ->
    Index = string:substr(Substring, 1, ?B + ?BETA),

    case dict:find(Index, Dict) of
        error ->
            dict:store(Index, [Sig], Dict);
        {ok, Matches} ->
            dict:store(Index, [Sig|Matches], Dict)
    end.

add_trie_index(Trie, Substring) ->
    [TrieIndex, Rest] = split(Substring, ?B, ?BETA),

    case dict:find(TrieIndex, Trie) of
        error ->
            dict_insert(Trie, #leaf_node{}, TrieIndex, Rest);
        {ok, Leaf} ->
            dict_insert(Trie, Leaf, TrieIndex, Rest)
    end.

split(Str, _, _) when length(Str) =< ?B ->
    [Str, []];
split(Str, Start, End) ->
    P1 = string:substr(Str, 1, Start),
    P2 = string:substr(Str, Start + 1, End),
    [P1, P2].

dict_insert(Trie, Leaf0, Index, SubStr) when length(SubStr) < ?BETA ->
    Matches0 = Leaf0#leaf_node.matches,
    Matches1 = [SubStr|Matches0],
    Leaf1 = Leaf0#leaf_node{matches=Matches1},
    dict:store(Index, Leaf1, Trie);
dict_insert(Trie, Leaf0, Index, SubStr) ->
    Bloomfilter = Leaf0#leaf_node.bloom,
    B1 = ?default(Bloomfilter, fun() -> bloom:sbf(26700) end),
    B2 = bloom:add_element(SubStr, B1),
    Leaf1 = Leaf0#leaf_node{bloom=B2},
    dict:store(Index, Leaf1, Trie).

%%%
%% @private
%% @doc
%% Constructs an effecient B-Gram cover given a signature set
%% @end
cover(Signatures) ->
    lists:reverse(cover(Signatures, [])).
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
    case representative_substring(?l2b(Sig), ?l2b(BGram)) of
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
    extract_covers_from_string(Sig, TabId),
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

-ifndef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% small patterns which are less than the size
%% of the representative substring are handled using
%% linked list lookups instead of bloom filters
small_pattern_test() ->
    % NOTE: we can't have patterns less than a ?B
    % characters since we don't use a trie. We index
    % possible matches by taking ?B characters from a string
    % and storing it as an index into an ETS table
    Patterns = [
        "cat",
        "cow",
        "dog",
        "i ",
        "xl van"
    ],
    S = sigmatch:new(Patterns),
    ?assertEqual(match, sigmatch:match("there is a cat in the bag", S)),
    ?assertEqual(match, sigmatch:match("cow", S)),
    ?assertEqual(match, sigmatch:match(" dog", S)),
    ?assertEqual(match, sigmatch:match("dog ", S)),
    ?assertEqual(match, sigmatch:match(" dog ", S)),
    ?assertEqual(match, sigmatch:match("fat cow", S)),
    ?assertEqual(match, sigmatch:match("foo i bar", S)),
    ?assertEqual(match, sigmatch:match("xl vanz", S)),
    ?assertEqual(nomatch, sigmatch:match("foobar", S)),
    ?assertEqual(nomatch, sigmatch:match("the lazy foo jumped over the moon", S)).

%% Bigger patterns will use bloom filters to check
%% for existence.
big_pattern_test() ->
    Patterns = [
        "will this big pattern match :D",
        "will this big pattern match ;D",
        "will this big pattern match :O"
    ],
    S = sigmatch:new(Patterns),

    % This will match because we look at ?B characters as the index
    % into our ETS then the following ?BETA characters are hashed into
    % a bloom filter checking for existence.
    ?assertEqual(match, sigmatch:match("will this big pattern match ;D", S)),
    ?assertEqual(nomatch, sigmatch:match("will this big pattern match ;X", S)).

-endif.
