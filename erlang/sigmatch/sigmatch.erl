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
%%%  +--------->|   SigMatch   |+------------->|    External   |
%%%             |              |+-------+      |   Validation  |
%%%             +--------------+        |      |               |
%%%                    +                |      +---------------+
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

-define(default(X, Y), case X of undefined -> Y; X -> X end).

% leaf nodes sit at the bottom of the truncated
% trie described in the SigMatch whitepaper. They
% will either contain a bloomfilter or a list of possible
% matches or both.
-record(leaf_node, {
    bloom,
    matches = []
}).

% tuning parameters for sigtree
-define(B, 2).
-define(BETA, 4).

% misc
-define(ALPHABET, 256).
-define(TABLE_SIZE, (?ALPHABET * ?ALPHABET)).
-define(BGRAM_SIZE, ?B + ?BETA).

%%%
%% @doc
%% new/1
%%
%% Given a list of signatures this function will generate
%% a sigtree
%% @end
new(Signatures) ->
    TabId = ets:new(sigtree, [ordered_set, {read_concurrency, true}]),
    Cover = cover(Signatures),
    build_index(TabId, Signatures, Cover),
    {sigtree, TabId}.

%%%
%% @doc
%% match/2
%%
%% Since were trying to match patterns against a text string
%% we need to walk the text character by characters until we hit
%% a match in our ETS ordered set.
%% @end
match(String, {sigtree, TabId}) ->
    walk_text(TabId, String).

walk_text(_, Str) when length(Str) =< ?B ->
    nomatch;
walk_text(TabId, [_|T] = String) ->
    [Chars, Rest] = split(String, ?B, ?BETA),
    TrieIndex = list_to_int(Chars),

    case ets:lookup(TabId, TrieIndex) of
        [{TrieIndex, Leaf}] ->
            case is_in_leaf(Leaf, Rest) of
                true ->
                    match;
                false ->
                    walk_text(TabId, T)
            end;
        [] ->
            walk_text(TabId, T)
    end.

is_in_leaf(#leaf_node{matches=Matches} = Leaf, String) ->
    case lists:any(fun(SubStr) -> is_prefix(SubStr, String) end, Matches) of
        true ->
            true;
        false ->
            check_bloom(Leaf, String)
    end.

is_prefix([], _) ->
    true; % the index is a pattern 
is_prefix(SubStr, String) ->
    string:str(String, SubStr) == 1.

check_bloom(#leaf_node{bloom=Bloom}, SubStr) ->
    case Bloom of
        undefined ->
            false;
        Bloom ->
            bloom:is_element(SubStr, Bloom)
    end.
    
build_index(_, [], _) ->
    ok;
build_index(TabId, [Sig|T], Cover) ->
    case walk_bgrams(TabId, Sig, Cover) of
        match ->
            ok;
        nomatch ->
            add_index(TabId, Sig)
    end,
    build_index(TabId, T, Cover).

walk_bgrams(_, _, []) ->
    nomatch;
walk_bgrams(TabId, Sig, [BGram | T]) ->
    case representative_substring(list_to_binary(Sig), BGram) of
        {match, Substring} ->
            add_index(TabId, binary_to_list(Substring)),
            match;
        nomatch ->
            walk_bgrams(TabId, Sig, T)
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
add_index(TabId, Substring) ->
    [Chars, Rest] = split(Substring, ?B, ?BETA),

    Index = list_to_int(Chars),

    case ets:lookup(TabId, Index) of
        [] ->
            ets_insert(TabId, #leaf_node{}, Index, Rest);
        [{Index, Leaf}] ->
            ets_insert(TabId, Leaf, Index, Rest)
    end.

split(Str, _, _) when length(Str) =< ?B ->
    [Str, []];
split(Str, Start, End) ->
    P1 = string:substr(Str, 1, Start),
    P2 = string:substr(Str, Start + 1, Start + End),
    [P1, P2].

ets_insert(TabId, Leaf0, Index, SubStr) when length(SubStr) < ?BETA ->
    Matches0 = Leaf0#leaf_node.matches,
    Matches1 = [SubStr|Matches0],
    Leaf1 = Leaf0#leaf_node{matches=Matches1},
    ets:insert(TabId, {Index, Leaf1});
ets_insert(TabId, Leaf0, Index, SubStr) ->
    X = Leaf0#leaf_node.bloom,
    B1 = ?default(X, bloom:bloom(4000)),
    B2 = bloom:add_element(SubStr, B1),
    Leaf1 = Leaf0#leaf_node{bloom=B2},
    ets:insert(TabId, {Index, Leaf1}).

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

left_to_right_pass(_Pos, Str, _TabId) when length(Str) < ?BGRAM_SIZE ->
    ok;
left_to_right_pass(Pos, [_|T] = Str, TabId) ->
    [Chars, _] = split(Str, ?B, ?BETA),
    Index = list_to_int(Chars),
    ets:update_counter(TabId, Index, 1),
    left_to_right_pass(Pos, T, TabId).

list_to_int(List) ->
    binary:decode_unsigned(list_to_binary(List)).
int_to_bin(Bin) ->
    binary:encode_unsigned(Bin).

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
        "i "
    ],
    S = sigmatch:new(Patterns),
    ?assertEqual(match, sigmatch:match("there is a cat in the bag", S)),
    ?assertEqual(match, sigmatch:match("cow", S)),
    ?assertEqual(match, sigmatch:match(" dog", S)),
    ?assertEqual(match, sigmatch:match("dog ", S)),
    ?assertEqual(match, sigmatch:match(" dog ", S)),
    ?assertEqual(match, sigmatch:match("fat cow", S)),
    ?assertEqual(match, sigmatch:match("foo i bar", S)),
    ?assertEqual(nomatch, sigmatch:match("foobar", S)),
    ?assertEqual(nomatch, sigmatch:match("the lazy foo jumped over the moon", S)).

-endif.
