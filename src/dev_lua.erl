%%% @doc A device that calls a Lua script upon a request and returns the result.
-module(dev_lua).
-export([info/1, init/3, snapshot/3, normalize/3, functions/3]).
%%% Public Utilities
-export([encode/2, decode/2]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").
%%% The set of functions that will be sandboxed by default if `sandbox` is set 
%%% to only `true`. Setting `sandbox` to a map allows the invoker to specify
%%% which functions should be sandboxed and what to return instead. Providing
%%% a list instead of a map will result in all functions being sandboxed and
%%% returning `sandboxed'.
-define(DEFAULT_SANDBOX, [
    {['_G', io], <<"sandboxed">>},
    {['_G', file], <<"sandboxed">>},
    {['_G', os, execute], <<"sandboxed">>},
    {['_G', os, exit], <<"sandboxed">>},
    {['_G', os, getenv], <<"sandboxed">>},
    {['_G', os, remove], <<"sandboxed">>},
    {['_G', os, rename], <<"sandboxed">>},
    {['_G', os, tmpname], <<"sandboxed">>},
    {['_G', package], <<"sandboxed">>},
    {['_G', loadfile], <<"sandboxed">>},
    {['_G', require], <<"sandboxed">>},
    {['_G', dofile], <<"sandboxed">>},
    {['_G', load], <<"sandboxed">>},
    {['_G', loadfile], <<"sandboxed">>},
    {['_G', loadstring], <<"sandboxed">>}
]).

%% @doc All keys that are not directly available in the base message are 
%% resolved by calling the Lua function in the script of the same name.
%% Additionally, we exclude the `keys', `set', `encode' and `decode' functions
%% which are `message@1.0' core functions, and Lua public utility functions.
info(Base) ->
    #{
        default => fun compute/4,
        excludes =>
            [<<"keys">>, <<"set">>, <<"encode">>, <<"decode">>]
                ++ maps:keys(Base)
    }.

%% @doc Initialize the device state, loading the script into memory if it is 
%% a reference.
init(Base, Req, Opts) ->
    ensure_initialized(Base, Req, Opts).

%% @doc Initialize the Lua VM if it is not already initialized. Optionally takes
%% the script as a  Binary string. If not provided, the script will be loaded
%% from the base message.
ensure_initialized(Base, _Req, Opts) ->
    case hb_private:from_message(Base) of
        #{<<"state">> := _} -> 
            ?event(debug_lua, lua_state_already_initialized),
            {ok, Base};
        _ ->
            ?event(debug_lua, initializing_lua_state),
            case find_scripts(Base, Opts) of
                {ok, Scripts} ->
                    initialize(Base, Scripts, Opts);
                Error ->
                    Error
            end
    end.

%% @doc Find the script in the base message, either by ID or by string.
find_scripts(Base, Opts) ->
    case hb_ao:get(<<"script">>, {as, <<"message@1.0">>, Base}, Opts) of
        not_found ->
            {error, <<"no-scripts-found">>};
        Script when is_binary(Script) ->
            find_scripts(Base#{ <<"script">> => [Script] }, Opts);
        Script when is_map(Script) ->
            % If the script is a map, check its content type to see if it is 
            % a literal Lua script, or a map of scripts with content types.
            case hb_ao:get(<<"content-type">>, Script, Opts) of
                CT when CT == <<"application/lua">> orelse CT == <<"text/x-lua">> ->
                    find_scripts(Base#{ <<"script">> => [Script] }, Opts);
                _ ->
                    % If the script is not a literal Lua script, assume it is a
                    % map of scripts with content types, and recurse.
                    find_scripts(Base#{ <<"script">> => maps:values(Script) }, Opts)
            end;
        Scripts when is_list(Scripts) ->
            % We have found a list of scripts, load them.
            load_scripts(Scripts, Opts)
    end.

%% @doc Load a list of scripts for installation into the Lua VM.
load_scripts(Scripts, Opts) -> load_scripts(Scripts, Opts, []).
load_scripts([], _Opts, Acc) ->
    {ok, lists:reverse(Acc)};
load_scripts([ScriptID | Rest], Opts, Acc) when is_binary(ScriptID) ->
    case hb_cache:read(ScriptID, Opts) of
        {ok, Script} ->
            load_scripts(Rest, Opts, [{ScriptID, Script}|Acc]);
        not_found ->
            {error, #{
                <<"status">> => 404,
                <<"body">> =>
                    <<"Lua script '", ScriptID/binary, "' not available.">>
            }}
    end;
load_scripts([Script | Rest], Opts, Acc) when is_map(Script) ->
    % We have found a message with a direct Lua script. Load it.
    case hb_ao:get(<<"body">>, Script, Opts) of
        not_found ->
            {error, #{
                <<"status">> => 404,
                <<"body">> =>
                    <<
                        """
                        Lua script not loadable. Lua scripts must have a
                        `body' element set to a binary of the code to load.
                        """
                    >>,
                <<"script">> => Script
            }};
        ScriptBin ->
            % Get the `module' key from the script message if it exists, or 
            % return the script ID as the module name.
            Module =
                hb_ao:get_first(
                    [
                        {Script, <<"module">>},
                        {Script, <<"id">>}
                    ],
                    Script,
                    Opts
                ),
            % Load the script into the Lua state.
            load_scripts(Rest, Opts, [{Module, ScriptBin}|Acc])
    end.

%% @doc Initialize a new Lua state with a given base message and script.
initialize(Base, Scripts, Opts) ->
    State0 = luerl:init(),
    % Load each script into the Lua state.
    State1 =
        lists:foldl(
            fun({ScriptID, ScriptBin}, StateIn) ->
                {ok, _, StateOut} =
                    luerl:do_dec(
                        ScriptBin,
                        [{module, hb_util:list(ScriptID)}],
                        StateIn
                    ),
                StateOut
            end,
            State0,
            Scripts
        ),
    % Apply any sandboxing rules to the state.
    State2 =
        case hb_ao:get(<<"sandbox">>, {as, <<"message@1.0">>, Base}, false, Opts) of
            false -> State1;
            true -> sandbox(State1, ?DEFAULT_SANDBOX, Opts);
            Spec -> sandbox(State1, Spec, Opts)
        end,
    % Install the AO-Core Lua library into the state.
    {ok, State3} = dev_lua_lib:install(Base, State2, Opts),
    % Return the base message with the state added to it.
    {ok, hb_private:set(Base, <<"state">>, State3, Opts)}.

%%% @doc Return a list of all functions in the Lua environment.
functions(Base, _Req, Opts) ->
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            {error, not_found};
        State ->
            {ok, [Res], _S2} =
                luerl:do_dec(
                    <<
                        """
                        local __tests = {}
                        for k, v in pairs(_G) do
                            if type(v) == "function" then
                                table.insert(__tests, k)
                            end
                        end
                        return __tests
                        """
                    >>,
                    State
                ),
            {ok, hb_util:message_to_ordered_list(decode(Res, Opts))}
    end.

%% @doc Sandbox (render inoperable) a set of Lua functions. Each function is
%% referred to as if it is a path in AO-Core, with its value being what to 
%% return to the caller. For example, 'os.exit' would be referred to as
%% referred to as `os/exit'. If preferred, a list rather than a map may be
%% provided, in which case the functions all return `sandboxed'.
sandbox(State, Map, Opts) when is_map(Map) ->
    sandbox(State, maps:to_list(Map), Opts);
sandbox(State, [], _Opts) ->
    State;
sandbox(State, [{Path, Value} | Rest], Opts) ->
    {ok, NextState} = luerl:set_table_keys_dec(Path, Value, State),
    sandbox(NextState, Rest, Opts);
sandbox(State, [Path | Rest], Opts) ->
    {ok, NextState} = luerl:set_table_keys_dec(Path, <<"sandboxed">>, State),
    sandbox(NextState, Rest, Opts).

%% @doc Call the Lua script with the given arguments.
compute(Key, RawBase, Req, Opts) ->
    ?event(debug_lua, compute_called),
    {ok, Base} = ensure_initialized(RawBase, Req, Opts),
    ?event(debug_lua, ensure_initialized_done),
    % Get the state from the base message's private element.
    OldPriv = #{ <<"state">> := State } = hb_private:from_message(Base),
    % TODO: looks like the script is injected in multiple places, does the 
    % script need to be passed?
    % Get the Lua function to call from the base message.
    Function =
        hb_ao:get_first(
            [
                {Req, <<"body/function">>},
                {Req, <<"function">>},
                {{as, <<"message@1.0">>, Base}, <<"function">>}
            ],
            Key,
            Opts#{ hashpath => ignore }
        ),
    ?event(debug_lua, function_found),
    Params =
        hb_ao:get_first(
            [
                {Req, <<"body/parameters">>},
                {Req, <<"parameters">>},
                {{as, <<"message@1.0">>, Base}, <<"parameters">>}
            ],
            [
                hb_private:reset(Base),
                Req,
                #{}
            ],
            Opts#{ hashpath => ignore }
        ),
    ?event(debug_lua, parameters_found),
    % Resolve all hyperstate links
    ResolvedParams = hb_cache:ensure_all_loaded(Params, Opts),
    % Call the VM function with the given arguments.
    ?event(lua,
        {calling_lua_func,
            {function, Function},
            {args, ResolvedParams},
            {req, Req}
        }
    ),
    process_response(
        try luerl:call_function_dec(
            [Function],
            encode(ResolvedParams, Opts),
            State
        )
        catch
            _:Reason:Stacktrace -> {error, Reason, Stacktrace}
        end,
        OldPriv,
		Opts
    ).

%% @doc Process a response to a Luerl invocation. Returns the typical AO-Core
%% HyperBEAM response format.
process_response({ok, [Result], NewState}, Priv, Opts) ->
    process_response({ok, [<<"ok">>, Result], NewState}, Priv, Opts);
process_response({ok, [Status, MsgResult], NewState}, Priv, Opts) ->
    % If the result is a HyperBEAM device return (`{Status, Msg}'), decode it 
    % and add the previous `priv' element back into the resulting message.
    case decode(MsgResult, Opts) of
        Msg when is_map(Msg) ->
            {hb_util:atom(Status), Msg#{
                <<"priv">> => Priv#{
                    <<"state">> => NewState
                }
            }};
        NonMsgRes -> {hb_util:atom(Status), NonMsgRes}
    end;
process_response({lua_error, RawError, State}, _Priv, Opts) ->
    % An error occurred while calling the Lua function. Parse the stack trace
    % and return it.
    Error = try decode(luerl:decode(RawError, State), Opts) catch _:_ -> RawError end,
    StackTrace = decode_stacktrace(luerl:get_stacktrace(State), State, Opts),
    ?event(lua_error, {lua_error, Error, {stacktrace, StackTrace}}),
    {error, #{
        <<"status">> => 500,
        <<"body">> => Error,
        <<"trace">> => hb_ao:normalize_keys(StackTrace, Opts)
    }};
process_response({error, Reason, Trace}, _Priv, _Opts) ->
    % An Erlang error occurred while calling the Lua function. Return it.
    ?event(lua_error, {trace, Trace}),
    TraceBin = iolist_to_binary(hb_util:format_trace(Trace)),
    ?event(lua_error, {formatted, TraceBin}),
    ReasonBin = iolist_to_binary(io_lib:format("~p", [Reason])),
    {error, #{
        <<"status">> => 500,
        <<"body">> =>
            << "Erlang error while running Lua: ", ReasonBin/binary >>,
        <<"trace">> => TraceBin
    }}.

%% @doc Snapshot the Lua state from a live computation. Normalizes its `priv'
%% state element, then serializes the state to a binary.
snapshot(Base, _Req, Opts) ->
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            {error, <<"Cannot snapshot Lua state: state not initialized.">>};
        State ->
            {ok,
                #{
                    <<"body">> =>
                        term_to_binary(luerl:externalize(State))
                }
            }
    end.

%% @doc Restore the Lua state from a snapshot, if it exists.
normalize(Base, _Req, RawOpts) ->
    Opts = RawOpts#{ hashpath => ignore },
    case hb_private:get(<<"state">>, Base, Opts) of
        not_found ->
            DeviceKey =
                case hb_ao:get(<<"device-key">>, {as, <<"message@1.0">>, Base}, Opts) of
                    not_found -> [];
                    Key -> [Key]
                end,
            ?event(
                {attempting_to_restore_lua_state,
                    {msg1, Base}, {device_key, DeviceKey}
                }
            ),
            SerializedState =
                hb_ao:get(
                    [<<"snapshot">>] ++ DeviceKey ++ [<<"body">>],
                    {as, dev_message, Base},
                    Opts
                ),
            case SerializedState of
                not_found -> throw({error, no_lua_state_snapshot_found});
                State ->
                    ExternalizedState = binary_to_term(State),
                    InternalizedState = luerl:internalize(ExternalizedState),
                    {ok, hb_private:set(Base, <<"state">>, InternalizedState, Opts)}
            end;
        _ ->
            ?event(state_already_initialized),
            {ok, Base}
    end.

%% @doc Decode a Lua result into a HyperBEAM `structured@1.0' message.
decode(EncMsg = [{_K, _V} | _], Opts) when is_list(EncMsg) ->
    decode(maps:map(fun(_, V) -> decode(V, Opts) end, maps:from_list(EncMsg)), Opts);
decode(Msg, Opts) when is_map(Msg) ->
    % If the message is an ordered list encoded as a map, decode it to a list.
    case hb_util:is_ordered_list(Msg, Opts) of
        true ->
            lists:map(fun(V) -> decode(V, Opts) end, hb_util:message_to_ordered_list(Msg));
        false ->
            Msg
    end;
decode(Other, _Opts) ->
    Other.

%% @doc Encode a HyperBEAM `structured@1.0' message into a Lua term.
encode(Map, Opts) when is_map(Map) ->
    case hb_util:is_ordered_list(Map, Opts) of
        true -> encode(hb_util:message_to_ordered_list(Map), Opts);
        false -> maps:to_list(maps:map(fun(_, V) -> encode(V, Opts) end, Map))
    end;
encode(List, _Opts) when is_list(List) ->
    lists:map(fun(V) -> encode(V, _Opts) end, List);
encode(Atom, _Opts) when is_atom(Atom) and (Atom /= false) and (Atom /= true)->
    hb_util:bin(Atom);
encode(Other, _Opts) ->
    Other.

%% @doc Parse a Lua stack trace into a list of messages.
decode_stacktrace(StackTrace, State0, Opts) ->
    decode_stacktrace(StackTrace, State0, [], Opts).
decode_stacktrace([], _State, Acc, _Opts) ->
    lists:reverse(Acc);
decode_stacktrace([{FuncBin, ParamRefs, FileInfo} | Rest], State0, Acc, Opts) ->
    %% Decode all the Lua table refs into Erlang terms
    DecodedParams = decode_params(ParamRefs, State0, Opts),
    %% Pull out the line number
    Line = proplists:get_value(line, FileInfo),
    File = proplists:get_value(file, FileInfo, undefined),
    ?event(debug_lua_stack, {stack_file, FileInfo}),
    %% Build our message‐map
    Entry = #{
        <<"function">>   => FuncBin,
        <<"parameters">> => hb_util:list_to_numbered_map(DecodedParams)
    },
    MaybeLine =
        if is_binary(File) andalso is_integer(Line) ->
            #{
                <<"line">> =>
                    iolist_to_binary(
                        io_lib:format("~s:~p", [File, Line])
                    )
            };
        is_integer(Line) ->
            #{ <<"line">> => Line };
        true ->
            #{}
        end,
    decode_stacktrace(Rest, State0, [maps:merge(Entry, MaybeLine)|Acc], Opts).

%% @doc Decode a list of Lua references, as found in a stack trace, into a
%% list of Erlang terms.
decode_params([], _State, _Opts) -> [];
decode_params([Tref|Rest], State, Opts) ->
    Decoded = decode(luerl:decode(Tref, State), Opts),
    [Decoded|decode_params(Rest, State, Opts)].

%%% Tests
simple_invocation_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => []
    },
    ?assertEqual(2, hb_ao:get(<<"assoctable/b">>, Base, #{})).


multiple_scripts_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Script2 =
        <<
            """
            function test_second_script()
                return 4
            end
            """
        >>,
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => [
            #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => Script
            },
            #{
                <<"content-type">> => <<"application/lua">>,
                <<"body">> => Script2
            }
        ],
        <<"parameters">> => []
    },
    ?assertEqual(2, hb_ao:get(<<"assoctable/b">>, Base, #{})),
    ?assertEqual(4, hb_ao:get(<<"test_second_script">>, Base, #{})).

error_response_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => []
    },
    ?assertEqual(
        {error, <<"Very bad, but Lua caught it.">>},
        hb_ao:resolve(Base, <<"error_response">>, #{})
    ).

sandboxed_failure_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => [],
        <<"sandbox">> => true
    },
    ?assertMatch({error, _}, hb_ao:resolve(Base, <<"sandboxed_fail">>, #{})).

%% @doc Run an AO-Core resolution from the Lua environment.
ao_core_sandbox_test_disabled() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => [],
        <<"device-sandbox">> => [<<"message@1.0">>]
    },
    ?assertMatch({error, _}, hb_ao:resolve(Base, <<"ao_relay">>, #{})),
    ?assertMatch({ok, _}, hb_ao:resolve(Base, <<"ao_resolve">>, #{})).

%% @doc Run an AO-Core resolution from the Lua environment.
ao_core_resolution_from_lua_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => []
    },
    {ok, Res} = hb_ao:resolve(Base, <<"ao_resolve">>, #{}),
    ?assertEqual(<<"Hello, AO world!">>, Res).

%% @doc Benchmark the performance of Lua executions.
direct_benchmark_test() ->
    BenchTime = 3,
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"parameters">> => []
    },
    Iterations = hb:benchmark(
        fun(X) ->
            {ok, _} = hb_ao:resolve(Base, <<"assoctable">>, #{}),
            ?event({iteration, X})
        end,
        BenchTime
    ),
    ?event({iterations, Iterations}),
    hb_util:eunit_print(
        "Computed ~p Lua executions in ~ps (~.2f calls/s)",
        [Iterations, BenchTime, Iterations / BenchTime]
    ),
    ?assert(Iterations > 10).

%% @doc Call a non-compute key on a Lua device message and ensure that the
%% function of the same name in the script is called.
invoke_non_compute_key_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Base = #{
        <<"device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"test-value">> => 42
    },
    {ok, Result1} = hb_ao:resolve(Base, <<"hello">>, #{}),
    ?event({result1, Result1}),
    ?assertEqual(42, hb_ao:get(<<"test-value">>, Result1, #{})),
    ?assertEqual(<<"world">>, hb_ao:get(<<"hello">>, Result1, #{})),
    {ok, Result2} =
        hb_ao:resolve(
            Base,
            #{<<"path">> => <<"hello">>, <<"name">> => <<"Alice">>},
            #{}
        ),
    ?event({result2, Result2}),
    ?assertEqual(<<"Alice">>, hb_ao:get(<<"hello">>, Result2, #{})).

%% @doc Use a Lua script as a preprocessor on the HTTP server via `~meta@1.0'.
lua_http_preprocessor_test() ->
    {ok, Script} = file:read_file("test/test.lua"),
    Node = hb_http_server:start_node(
        #{
            on => #{
                <<"request">> => #{
                    <<"device">> => <<"lua@5.3a">>,
                    <<"script">> => #{
                        <<"content-type">> => <<"application/lua">>,
                        <<"body">> => Script
                    }
                }
            }
        }),
    {ok, Res} = hb_http:get(Node, <<"/hello?hello=world">>, #{}),
    ?assertMatch(#{ <<"body">> := <<"i like turtles">> }, Res).

%% @doc Call a process whose `execution-device' is set to `lua@5.3a'.
pure_lua_process_test() ->
    Process = generate_lua_process("test/test.lua"),
    {ok, _} = hb_cache:write(Process, #{}),
    Message = generate_test_message(Process),
    {ok, _} = hb_ao:resolve(Process, Message, #{ hashpath => ignore }),
    {ok, Results} = hb_ao:resolve(Process, <<"now">>, #{}),
    ?assertEqual(42, hb_ao:get(<<"results/output/body">>, Results, #{})).

pure_lua_process_benchmark_test_() ->
    {timeout, 30, fun() ->
        BenchMsgs = 200,
        Process = generate_lua_process("test/test.lua"),
        Message = generate_test_message(Process),
        lists:foreach(
            fun(X) ->
                hb_ao:resolve(Process, Message, #{ hashpath => ignore }),
                ?event(debug_lua, {scheduled, X})
            end,
            lists:seq(1, BenchMsgs)
        ),
        ?event(debug_lua, {executing, BenchMsgs}),
        BeforeExec = os:system_time(millisecond),
        {ok, _} = hb_ao:resolve(
            Process,
            <<"now">>,
            #{ hashpath => ignore, process_cache_frequency => 50 }
        ),
        AfterExec = os:system_time(millisecond),
        ?event(debug_lua, {execution_time, (AfterExec - BeforeExec) / BenchMsgs}),
        hb_util:eunit_print(
            "Computed ~p pure Lua process executions in ~ps (~.2f calls/s)",
            [
                BenchMsgs,
                (AfterExec - BeforeExec) / 1000,
                BenchMsgs / ((AfterExec - BeforeExec) / 1000)
            ]
        )
    end}.

invoke_aos_test() ->
    Process = generate_lua_process("test/hyper-aos.lua"),
    {ok, _} = hb_cache:write(Process, #{}),
    Message = generate_test_message(Process),
    {ok, _} = hb_ao:resolve(Process, Message, #{ hashpath => ignore }),
    {ok, Results} = hb_ao:resolve(Process, <<"now/results/output/data">>, #{}),
    ?assertEqual(<<"1">>, Results).

aos_authority_not_trusted_test() ->
    Process = generate_lua_process("test/hyper-aos.lua"),
    ProcID = hb_message:id(Process, all),
    {ok, _} = hb_cache:write(Process, #{}),
    GuestWallet = ar_wallet:new(),
    Message = hb_message:commit(#{
        <<"path">> => <<"schedule">>,
        <<"method">> => <<"POST">>,
        <<"body">> =>
            hb_message:commit(
                #{
                    <<"target">> => ProcID,
                    <<"type">> => <<"Message">>,
                    <<"data">> => <<"1 + 1">>,
                    <<"random-seed">> => rand:uniform(1337),
                    <<"action">> => <<"Eval">>,
                    <<"from-process">> => <<"1234">>

        }, GuestWallet)
      }, GuestWallet
    ),
    {ok, _} = hb_ao:resolve(Process, Message, #{ hashpath => ignore }),
    {ok, Results} = hb_ao:resolve(Process, <<"now/results/output/data">>, #{}),
    ?assertEqual(<<"Message is not trusted.">>, Results).

%% @doc Benchmark the performance of Lua executions.
aos_process_benchmark_test_() ->
    {timeout, 30, fun() ->
        BenchMsgs = 200,
        Process = generate_lua_process("test/hyper-aos.lua"),
        Message = generate_test_message(Process),
        lists:foreach(
            fun(X) ->
                hb_ao:resolve(Process, Message, #{ hashpath => ignore }),
                ?event(debug_lua, {scheduled, X})
            end,
            lists:seq(1, BenchMsgs)
        ),
        ?event(debug_lua, {executing, BenchMsgs}),
        BeforeExec = os:system_time(millisecond),
        {ok, _} = hb_ao:resolve(
            Process,
            <<"now">>,
            #{ hashpath => ignore, process_cache_frequency => 50 }
        ),
        AfterExec = os:system_time(millisecond),
        ?event(debug_lua, {execution_time, (AfterExec - BeforeExec) / BenchMsgs}),
        hb_util:eunit_print(
            "Computed ~p AOS process executions in ~ps (~.2f calls/s)",
            [
                BenchMsgs,
                (AfterExec - BeforeExec) / 1000,
                BenchMsgs / ((AfterExec - BeforeExec) / 1000)
            ]
        )
    end}.

%%% Test helpers

%% @doc Generate a Lua process message.
generate_lua_process(File) ->
    Wallet = hb:wallet(),
    {ok, Script} = file:read_file(File),
    hb_message:commit(#{
        <<"device">> => <<"process@1.0">>,
        <<"type">> => <<"Process">>,
        <<"scheduler-device">> => <<"scheduler@1.0">>,
        <<"execution-device">> => <<"lua@5.3a">>,
        <<"script">> => #{
            <<"content-type">> => <<"application/lua">>,
            <<"body">> => Script
        },
        <<"authority">> => [ 
          hb:address(), 
          <<"E3FJ53E6xtAzcftBpaw2E1H4ZM9h6qy6xz9NXh5lhEQ">>
        ], 
        <<"scheduler-location">> =>
            hb_util:human_id(ar_wallet:to_address(Wallet)),
        <<"test-random-seed">> => rand:uniform(1337)
    }, Wallet).

%% @doc Generate a test message for a Lua process.
generate_test_message(Process) ->
    ProcID = hb_message:id(Process, all),
    Wallet = hb:wallet(),
    Code = """ 
      Count = 0
      function add() 
        Send({Target = 'Foo', Data = 'Bar' });
        Count = Count + 1 
      end
      add()
      return Count
    """,
    hb_message:commit(#{
            <<"path">> => <<"schedule">>,
            <<"method">> => <<"POST">>,
            <<"body">> =>
                hb_message:commit(
                    #{
                        <<"target">> => ProcID,
                        <<"type">> => <<"Message">>,
                        <<"body">> => #{
                            <<"content-type">> => <<"application/lua">>,
                            <<"body">> => list_to_binary(Code) 
                        },
                        <<"random-seed">> => rand:uniform(1337),
                        <<"action">> => <<"Eval">>
                    },
                    Wallet
                )
        },
        Wallet
    ).

%% @doc Generate a stack message for the Lua process.
generate_stack(File) ->
    Wallet = hb:wallet(),
    {ok, Script} = file:read_file(File),
    Msg1 = #{
        <<"device">> => <<"Stack@1.0">>,
        <<"device-stack">> =>
            [
                <<"json-iface@1.0">>,
                <<"lua@5.3a">>,
                <<"multipass@1.0">>
            ],
        <<"function">> => <<"json_result">>,
        <<"passes">> => 2,
        <<"stack-keys">> => [<<"init">>, <<"compute">>],
        <<"script">> => Script,
        <<"process">> => 
            hb_message:commit(#{
                <<"type">> => <<"Process">>,
                <<"script">> => #{
                    <<"content-type">> => <<"application/lua">>,
                    <<"body">> => Script
                },
                <<"scheduler">> => hb:address(),
                <<"authority">> => hb:address()
            }, Wallet)
    },
    {ok, Msg2} = hb_ao:resolve(Msg1, <<"init">>, #{}),
    Msg2.

execute_aos_call(Base) ->
    Req =
        hb_message:commit(#{
                <<"action">> => <<"Eval">>,
                <<"function">> => <<"json_result">>,
                <<"data">> => <<"return 2">>
            },
            hb:wallet()
        ),
    execute_aos_call(Base, Req).
execute_aos_call(Base, Req) ->
    hb_ao:resolve(Base,
        #{
            <<"path">> => <<"compute">>,
            <<"body">> => Req
        },
        #{}
    ).