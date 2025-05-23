%%% Hyperbeam's core HTTP request/reply functionality. The functions in this
%%% module generally take a message request from their caller and return a
%%% response in message form, as granted by the peer. This module is mostly
%%% used by hb_client, but can also be used by other modules that need to make
%%% HTTP requests.
-module(hb_http).
-export([start/0]).
-export([get/2, get/3, post/3, post/4, request/2, request/4, request/5]).
-export([reply/4, accept_to_codec/2]).
-export([req_to_tabm_singleton/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

start() ->
    httpc:set_options([{max_keep_alive_length, 0}]),
    ok.

%% @doc Gets a URL via HTTP and returns the resulting message in deserialized
%% form.
get(Node, Opts) -> get(Node, <<"/">>, Opts).
get(Node, PathBin, Opts) when is_binary(PathBin) ->
    get(Node, #{ <<"path">> => PathBin }, Opts);
get(Node, Message, Opts) ->
    request(
        <<"GET">>,
        Node,
        hb_ao:get(<<"path">>, Message, <<"/">>, Opts),
        Message,
        Opts
    ).

%% @doc Posts a message to a URL on a remote peer via HTTP. Returns the
%% resulting message in deserialized form.
post(Node, Message, Opts) ->
    post(Node,
        hb_ao:get(
            <<"path">>,
            Message,
            <<"/">>,
            Opts#{ topic => ao_internal }
        ),
        Message,
        Opts
    ).
post(Node, Path, Message, Opts) ->
    case request(<<"POST">>, Node, Path, Message, Opts) of
        {ok, Res} ->
            ?event(http, {post_response, Res}),
            {ok, Res};
        Error -> Error
    end.

%% @doc Posts a binary to a URL on a remote peer via HTTP, returning the raw
%% binary body.
request(Message, Opts) ->
    % Special case: We are not given a peer and a path, so we need to
    % preprocess the URL to find them.
    {ok, Method, Peer, Path, MessageToSend, NewOpts} =
        message_to_request(Message, Opts),
    request(Method, Peer, Path, MessageToSend, NewOpts).
request(Method, Peer, Path, Opts) ->
    request(Method, Peer, Path, #{}, Opts).
request(Method, Config = #{ <<"nodes">> := Nodes }, Path, Message, Opts) when is_list(Nodes) ->
    % The request has a `route' (see `dev_router' for more details), so we use the
    % `multirequest' functionality, rather than a single request.
    multirequest(Config, Method, Path, Message, Opts);
request(Method, #{ <<"opts">> := NodeOpts, <<"uri">> := URI }, _Path, Message, Opts) ->
    % The request has a set of additional options, so we apply them to the
    % request.
    MergedOpts = hb_maps:merge(Opts, NodeOpts, Opts),
    % We also recalculate the request. The order of precidence here is subtle:
    % We favor the args given to the function, but the URI rules take precidence
    % over that.
    {ok, NewMethod, Node, NewPath, NewMsg, NewOpts} =
        message_to_request(
            Message#{ <<"path">> => URI, <<"method">> => Method },
            MergedOpts
        ),
    request(NewMethod, Node, NewPath, NewMsg, NewOpts);
request(Method, Peer, Path, RawMessage, Opts) ->
    ?event({request, {method, Method}, {peer, Peer}, {path, Path}, {message, RawMessage}}),
    Req =
        prepare_request(
            hb_ao:get(
                <<"codec-device">>,
                RawMessage,
                <<"httpsig@1.0">>,
                Opts
            ),
            Method,
            Peer,
            Path,
            RawMessage,
            Opts
        ),
    StartTime = os:system_time(millisecond),
    {_ErlStatus, Status, Headers, Body} = hb_http_client:req(Req, Opts),
    EndTime = os:system_time(millisecond),
    ?event(http_outbound,
        {
            http_response,
            {req, Req},
            {response,
                #{
                    status => Status,
                    headers => Headers,
                    body => Body
                }
            }
        },
        Opts
    ),
    HeaderMap = hb_maps:from_list(Headers),
    NormHeaderMap = hb_ao:normalize_keys(HeaderMap, Opts),
    ?event(http_outbound,
        {normalized_response_headers, {norm_header_map, NormHeaderMap}},
        Opts
    ),
    BaseStatus =
        case Status of
            201 -> created;
            X when X < 400 -> ok;
            X when X < 500 -> error;
            _ -> failure
        end,
    ?event(http_short,
        {received,
            {status, Status},
            {duration, EndTime - StartTime},
            {method, Method},
            {peer, Peer},
            {path, {string, Path}},
            {body_size, byte_size(Body)}
        }),
    case hb_maps:get(<<"ao-result">>, NormHeaderMap, undefined, Opts) of
        Key when is_binary(Key) ->
            Msg = http_response_to_httpsig(Status, NormHeaderMap, Body, Opts),
            ?event(http_outbound, {result_is_single_key, {key, Key}, {msg, Msg}}, Opts),
            case hb_maps:get(Key, Msg, undefined, Opts) of
                undefined ->
                    {failure,
                        <<
                            "Result key '",
                            Key/binary,
                            "' not found in response from '",
                            Peer/binary,
                            "' for path '",
                            Path/binary,
                            "': ",
                            Body/binary
                        >>
                    };
                Value -> {BaseStatus, Value}
            end;
        undefined ->
            case hb_maps:get(<<"codec-device">>, NormHeaderMap, <<"httpsig@1.0">>, Opts) of
                <<"httpsig@1.0">> ->
                    ?event(http_outbound, {result_is_httpsig, {body, Body}}, Opts),
                    {
                        BaseStatus,
                        http_response_to_httpsig(Status, NormHeaderMap, Body, Opts)
                    };
                <<"ans104@1.0">> ->
                    ?event(http_outbound, {result_is_ans104, {body, Body}}, Opts),
                    Deserialized = ar_bundles:deserialize(Body),
                    % We don't need to add the status to the message, because
                    % it is already present in the encoded ANS-104 message.
                    {
                        BaseStatus,
                        hb_message:convert(
                            Deserialized,
                            <<"structured@1.0">>,
                            <<"ans104@1.0">>,
                            Opts
                        )
                    }
            end
    end.

%% @doc Convert a HTTP response to a httpsig message.
http_response_to_httpsig(Status, HeaderMap, Body, Opts) ->
    (hb_message:convert(
        hb_maps:merge(
            HeaderMap#{ <<"status">> => hb_util:bin(Status) },
            case Body of
                <<>> -> #{};
                _ -> #{ <<"body">> => Body }
            end,
			Opts
        ),
        <<"structured@1.0">>,
        <<"httpsig@1.0">>,
        Opts#{always_bundle => true}
    ))#{ <<"status">> => hb_util:int(Status) }.

%% @doc Given a message, return the information needed to make the request.
message_to_request(M, Opts) ->
    % Get the route for the message
    Res = route_to_request(M, RouteRes = dev_router:route(M, Opts), Opts),
    ?event(debug_http, {route_res, {route_res, RouteRes}, {full_res, Res}, {msg, M}}),
    Res.

%% @doc Parse a `dev_router:route' response and return a tuple of request
%% parameters.
route_to_request(M, {ok, URI}, Opts) when is_binary(URI) ->
    route_to_request(M, {ok, #{ <<"uri">> => URI, <<"opts">> => #{} }}, Opts);
route_to_request(M, {ok, #{ <<"uri">> := XPath, <<"opts">> := ReqOpts}}, Opts) ->
    % The request is a direct HTTP URL, so we need to split the path into a
    % host and path.
    URI = uri_string:parse(XPath),
    ?event(http_outbound, {parsed_uri, {uri, {explicit, URI}}}),
    Method = hb_ao:get(<<"method">>, M, <<"GET">>, Opts),
    % We must remove the path and host from the message, because they are not
    % valid for outbound requests. The path is retrieved from the route, and
    % the host should already be known to the caller.
    MsgWithoutMeta = hb_maps:without([<<"path">>, <<"host">>], M, Opts),
    Port =
        case hb_maps:get(port, URI, undefined, Opts) of
            undefined ->
                % If no port is specified, use 80 for HTTP and 443
                % for HTTPS.
                case XPath of
                    <<"https", _/binary>> -> <<"443">>;
                    _ -> <<"80">>
                end;
            X -> integer_to_binary(X)
        end,
    Protocol = hb_maps:get(scheme, URI, <<"https">>, Opts),
    Host = hb_maps:get(host, URI, <<"localhost">>, Opts),
    Node = << Protocol/binary, "://", Host/binary, ":", Port/binary  >>,
    PathParts = [hb_maps:get(path, URI, <<"/">>, Opts)] ++
        case hb_maps:get(query, URI, <<>>, Opts) of
            <<>> -> [];
            Query -> [<<"?", Query/binary>>]
        end,
    Path = iolist_to_binary(PathParts),
    ?event(http_outbound, {parsed_req, {node, Node}, {method, Method}, {path, Path}}),
    {ok, Method, Node, Path, MsgWithoutMeta, hb_util:deep_merge(Opts, ReqOpts, Opts)};
route_to_request(M, {ok, Routes}, Opts) ->
    ?event(http_outbound, {found_routes, {req, M}, {routes, Routes}}),
    % The result is a route, so we leave it to `request' to handle it.
    Path = hb_ao:get(<<"path">>, M, <<"/">>, Opts),
    Method = hb_ao:get(<<"method">>, M, <<"GET">>, Opts),
    % We must remove the path and host from the message, because they are not
    % valid for outbound requests. The path is retrieved from the route, and
    % the host should already be known to the caller.
    MsgWithoutMeta = hb_maps:without([<<"path">>, <<"host">>], M, Opts),
    {ok, Method, Routes, Path, MsgWithoutMeta, Opts};
route_to_request(M, {error, Reason}, _Opts) ->
    {error, {no_viable_route, {reason, Reason}, {message, M}}}.

%% @doc Turn a set of request arguments into a request message, formatted in the
%% preferred format.
prepare_request(Format, Method, Peer, Path, RawMessage, Opts) ->
    Message = hb_ao:normalize_keys(RawMessage, Opts),
    BinPeer = if is_binary(Peer) -> Peer; true -> list_to_binary(Peer) end,
    BinPath = hb_path:normalize(hb_path:to_binary(Path)),
    ReqBase = #{ peer => BinPeer, path => BinPath, method => Method },
    case Format of
        <<"httpsig@1.0">> ->
            FullEncoding =
                hb_message:convert(Message, <<"httpsig@1.0">>, Opts#{always_bundle => true}),
            Body = hb_maps:get(<<"body">>, FullEncoding, <<>>, Opts),
            Headers = hb_maps:without([<<"body">>], FullEncoding, Opts),

			?event(http, {request_headers, {explicit, {headers, Headers}}}),
			?event(http, {request_body, {explicit, {body, Body}}}),
            hb_maps:merge(ReqBase, #{ headers => Headers, body => Body }, Opts);
        <<"ans104@1.0">> ->
            ReqBase#{
                headers =>
                    #{
                        <<"codec-device">> => <<"ans104@1.0">>,
                        <<"content-type">> => <<"application/ans104">>
                    },
                body =>
                    ar_bundles:serialize(
                        hb_message:convert(Message, <<"ans104@1.0">>, Opts)
                    )
            };
        _ ->
            ReqBase#{
                headers => maps:without([<<"body">>], Message),
                body => maps:get(<<"body">>, Message, <<>>)
            }
    end.

%% @doc Dispatch the same HTTP request to many nodes. Can be configured to
%% await responses from all nodes or just one, and to halt all requests after
%% after it has received the required number of responses, or to leave all
%% requests running until they have all completed. Default: Race for first
%% response.
%%
%% Expects a config message of the following form:
%%      /Nodes/1..n: Hostname | #{ hostname => Hostname, address => Address }
%%      /Responses: Number of responses to gather
%%      /Stop-After: Should we stop after the required number of responses?
%%      /Parallel: Should we run the requests in parallel?
multirequest(Config, Method, Path, Message, Opts) ->
    MultiOpts = #{
        nodes := Nodes,
        responses := Responses,
        stop_after := StopAfter,
        accept_status := Statuses,
        parallel := Parallel
    } = multirequest_opts(Config, Message, Opts),
    ?event(http,
        {multirequest_opts_parsed,
            {config, Config},
            {message, Message},
            {multirequest_opts, MultiOpts}
        }),
    AllResults =
        if Parallel ->
            parallel_multirequest(
                Nodes, Responses, StopAfter, Method, Path, Message, Statuses, Opts);
        true ->
            serial_multirequest(
                Nodes, Responses, Method, Path, Message, Statuses, Opts)
        end,
    ?event(http, {multirequest_results, {results, AllResults}}),
    case AllResults of
        [] -> {error, no_viable_responses};
        Results -> if Responses == 1 -> hd(Results); true -> Results end
    end.

%% @doc Get the multirequest options from the config or message. The options in 
%% the message take precidence over the options in the config.
multirequest_opts(Config, Message, Opts) ->
    Opts#{
        nodes =>
            multirequest_opt(<<"nodes">>, Config, Message, #{}, Opts),
        responses =>
            multirequest_opt(<<"responses">>, Config, Message, 1, Opts),
        stop_after =>
            multirequest_opt(<<"stop-after">>, Config, Message, true, Opts),
        accept_status =>
            multirequest_opt(<<"accept-status">>, Config, Message, <<"All">>, Opts),
        parallel =>
            multirequest_opt(<<"parallel">>, Config, Message, false, Opts)
    }.

%% @doc Get a value for a multirequest option from the config or message.
multirequest_opt(Key, Config, Message, Default, Opts) ->
    hb_ao:get_first(
        [
            {Message, <<"multirequest-", Key/binary>>},
            {Config, Key}
        ],
        Default,
        Opts#{ hashpath => ignore }
    ).

%% @doc Serially request a message, collecting responses until the required
%% number of responses have been gathered. Ensure that the statuses are
%% allowed, according to the configuration.
serial_multirequest(_Nodes, 0, _Method, _Path, _Message, _Statuses, _Opts) -> [];
serial_multirequest([], _, _Method, _Path, _Message, _Statuses, _Opts) -> [];
serial_multirequest([Node|Nodes], Remaining, Method, Path, Message, Statuses, Opts) ->
    {ErlStatus, Res} = request(Method, Node, Path, Message, Opts),
    BaseStatus = hb_ao:get(<<"status">>, Res, Opts),
    case (ErlStatus == ok) andalso allowed_status(BaseStatus, Statuses) of
        true ->
            ?event(http, {admissible_status, {response, Res}}),
            [
                {ErlStatus, Res}
            |
                serial_multirequest(Nodes, Remaining - 1, Method, Path, Message, Statuses, Opts)
            ];
        false ->
            ?event(http, {inadmissible_status, {response, Res}}),
            serial_multirequest(Nodes, Remaining, Method, Path, Message, Statuses, Opts)
    end.

%% @doc Dispatch the same HTTP request to many nodes in parallel.
parallel_multirequest(Nodes, Responses, StopAfter, Method, Path, Message, Statuses, Opts) ->
    Ref = make_ref(),
    Parent = self(),
    Procs = lists:map(
        fun(Node) ->
            spawn(
                fun() ->
                    Res = request(Method, Node, Path, Message, Opts),
                    receive no_reply -> stopping
                    after 0 -> Parent ! {Ref, self(), Res}
                    end
                end
            )
        end,
        Nodes
    ),
    parallel_responses([], Procs, Ref, Responses, StopAfter, Statuses, Opts).

%% @doc Check if a status is allowed, according to the configuration.
allowed_status(_, <<"All">>) -> true;
allowed_status(_ResponseMsg = #{ <<"status">> := Status }, Statuses) ->
    allowed_status(Status, Statuses);
allowed_status(Status, Statuses) when is_integer(Statuses) ->
    allowed_status(Status, [Statuses]);
allowed_status(Status, Statuses) when is_binary(Status) ->
    allowed_status(binary_to_integer(Status), Statuses);
allowed_status(Status, Statuses) when is_binary(Statuses) ->
    % Convert the statuses to a list of integers.
    allowed_status(
        Status,
        lists:map(fun binary_to_integer/1, binary:split(Statuses, <<",">>))
    );
allowed_status(Status, Statuses) when is_list(Statuses) ->
    lists:member(Status, Statuses).

%% @doc Collect the necessary number of responses, and stop workers if
%% configured to do so.
parallel_responses(Res, Procs, Ref, 0, false, _Statuses, _Opts) ->
    lists:foreach(fun(P) -> P ! no_reply end, Procs),
    empty_inbox(Ref),
    {ok, Res};
parallel_responses(Res, Procs, Ref, 0, true, _Statuses, _Opts) ->
    lists:foreach(fun(P) -> exit(P, kill) end, Procs),
    empty_inbox(Ref),
    Res;
parallel_responses(Res, Procs, Ref, Awaiting, StopAfter, Statuses, Opts) ->
    receive
        {Ref, Pid, {Status, NewRes}} ->
            case allowed_status(Status, Statuses) of
                true ->
                    parallel_responses(
                        [NewRes | Res],
                        lists:delete(Pid, Procs),
                        Ref,
                        Awaiting - 1,
                        StopAfter,
                        Statuses,
                        Opts
                    );
                false ->
                    parallel_responses(
                        Res,
                        lists:delete(Pid, Procs),
                        Ref,
                        Awaiting,
                        StopAfter,
                        Statuses,
                        Opts
                    )
            end
    end.

%% @doc Empty the inbox of the current process for all messages with the given
%% reference.
empty_inbox(Ref) ->
    receive {Ref, _} -> empty_inbox(Ref) after 0 -> ok end.

%% @doc Reply to the client's HTTP request with a message.
reply(Req, TABMReq, Message, Opts) ->
    Status =
        case hb_ao:get(<<"status">>, Message, Opts) of
            not_found -> 200;
            S-> S
        end,
    reply(Req, TABMReq, Status, Message, Opts).
reply(Req, TABMReq, BinStatus, RawMessage, Opts) when is_binary(BinStatus) ->
    reply(Req, TABMReq, binary_to_integer(BinStatus), RawMessage, Opts);
reply(Req, TABMReq, Status, RawMessage, Opts) ->
    Message = hb_ao:normalize_keys(RawMessage, Opts),
    {ok, HeadersBeforeCors, EncodedBody} = encode_reply(TABMReq, Message, Opts),
    % Get the CORS request headers from the message, if they exist.
    ReqHdr = cowboy_req:header(<<"access-control-request-headers">>, Req, <<"">>),
    HeadersWithCors = add_cors_headers(HeadersBeforeCors, ReqHdr, Opts),
    EncodedHeaders = hb_private:reset(HeadersWithCors),
    ?event(http,
        {http_replying,
            {status, {explicit, Status}},
            {path, hb_maps:get(<<"path">>, Req, undefined_path, Opts)},
            {raw_message, RawMessage},
            {enc_headers, EncodedHeaders},
            {enc_body, EncodedBody}
        }
    ),
    % Cowboy handles cookies in headers separately, so we need to manipulate
    % the request to set the cookies such that they will be sent over the wire
    % unmodified.
    SetCookiesReq =
        case hb_maps:get(<<"set-cookie">>, EncodedHeaders, undefined, Opts) of
            undefined -> Req#{ resp_headers => EncodedHeaders };
            Cookies ->
                Req#{
                    resp_headers => EncodedHeaders,
                    resp_cookies => #{ <<"__HB_SET_COOKIE">> => Cookies }
                }
        end,
    Req2 = cowboy_req:stream_reply(Status, #{}, SetCookiesReq),
    cowboy_req:stream_body(EncodedBody, nofin, Req2),
    EndTime = os:system_time(millisecond),
    ?event(http, {reply_headers, {explicit, {ok, Req2, no_state}}}),
    ?event(http_short,
        {sent,
            {status, Status},
            {duration, EndTime - hb_maps:get(start_time, Req, undefined, Opts)},
            {method, cowboy_req:method(Req)},
            {path,
                {string,
                    uri_string:percent_decode(
                        hb_ao:get(<<"path">>, TABMReq, <<"[NO PATH]">>, Opts)
                    )
                }
            },
            {body_size, byte_size(EncodedBody)}
        }
    ),
    {ok, Req2, no_state}.

%% @doc Add permissive CORS headers to a message, if the message has not already
%% specified CORS headers.
add_cors_headers(Msg, ReqHdr, Opts) ->
    CorHeaders = #{
        <<"access-control-allow-origin">> => <<"*">>,
        <<"access-control-allow-methods">> => <<"GET, POST, PUT, DELETE, OPTIONS">>,
        <<"access-control-expose-headers">> => <<"*">>
    },
     WithAllowHeaders = case ReqHdr of
        <<>> -> CorHeaders;
        _ -> CorHeaders#{
             <<"access-control-allow-headers">> => ReqHdr
        }
    end,
    % Keys in the given message will overwrite the defaults listed below if 
    % included, due to `hb_maps:merge''s precidence order.
    hb_maps:merge(WithAllowHeaders, Msg, Opts).

%% @doc Generate the headers and body for a HTTP response message.
encode_reply(TABMReq, Message, Opts) ->
    Codec = accept_to_codec(TABMReq, Opts),
    ?event(http, {encoding_reply, {codec, Codec}, {message, Message}}),
    BaseHdrs =
        hb_maps:merge(
            #{
                <<"codec-device">> => Codec
            },
            case codec_to_content_type(Codec, Opts) of
                    undefined -> #{};
                    CT -> #{ <<"content-type">> => CT }
            end,
			Opts
        ),
    % Codecs generally do not need to specify headers outside of the content-type,
    % aside the default `httpsig@1.0' codec, which expresses its form in HTTP
    % documents, and subsequently must set its own headers.
    case Codec of
        <<"httpsig@1.0">> ->
            TABM =
                hb_message:convert(
                    Message,
                    tabm,
                    <<"structured@1.0">>,
                    Opts#{ topic => ao_internal }
                ),
            {ok, EncMessage} =
                dev_codec_httpsig:to(
                    TABM,
                    #{ <<"index">> => hb_opts:get(generate_index, true, Opts) },
                    Opts
                ),
            {
                ok,
                hb_maps:without([<<"body">>], EncMessage, Opts),
                hb_maps:get(<<"body">>, EncMessage, <<>>, Opts)
            };
        <<"ans104@1.0">> ->
            % The `ans104@1.0' codec is a binary format, so we must serialize
            % the message to a binary before sending it.
            {
                ok,
                BaseHdrs,
                ar_bundles:serialize(
                    hb_message:convert(
                        hb_message:with_only_committers(
                            Message,
                            hb_message:signers(Message, Opts),
							Opts
                        ),
                        <<"ans104@1.0">>,
                        <<"structured@1.0">>,
                        Opts#{ topic => ao_internal }
                    )
                )
            };
        _ ->
            % Other codecs are already in binary format, so we can just convert
            % the message to the codec. We also include all of the top-level 
            % fields in the message and return them as headers.
            ExtraHdrs = hb_maps:filter(fun(_, V) -> not is_map(V) end, Message, Opts),
            ?event({extra_headers, {headers, {explicit, ExtraHdrs}}, {message, Message}}),
            {ok,
                hb_maps:merge(BaseHdrs, ExtraHdrs, Opts),
                hb_message:convert(
                    Message,
                    Codec,
                    <<"structured@1.0">>,
                    Opts#{ topic => ao_internal }
                )
            }
    end.

%% @doc Calculate the codec name to use for a reply given its initiating Cowboy
%% request, the parsed TABM request, and the response message. The precidence
%% order for finding the codec is:
%% 1. The `accept-codec' field in the message
%% 2. The `accept' field in the request headers
%% 3. The default codec
%% Options can be specified in mime-type format (`application/*') or in
%% AO device format (`device@1.0').
accept_to_codec(TABMReq, Opts) ->
    AcceptCodec =
        hb_maps:get(
            <<"accept-codec">>,
            TABMReq,
            mime_to_codec(hb_maps:get(<<"accept">>, TABMReq, <<"*/*">>), Opts),
			Opts
        ),
    ?event(http, {accept_to_codec, AcceptCodec}),
    case AcceptCodec of
        not_specified ->
            % We hold off until confirming that the codec is not directly in the
            % message before calling `hb_opts:get/3', as it is comparatively
            % expensive.
            default_codec(Opts);
        _ -> AcceptCodec
    end.

%% @doc Find a codec name from a mime-type.
mime_to_codec(<<"application/", Mime/binary>>, Opts) ->
    Name =
        case binary:match(Mime, <<"@">>) of
            nomatch -> << Mime/binary, "@1.0" >>;
            _ -> Mime
        end,
    try hb_ao:message_to_device(#{ <<"device">> => Name }, Opts)
    catch _:Error ->
        ?event(http, {accept_to_codec_error, {name, Name}, {error, Error}}),
        default_codec(Opts)
    end;
mime_to_codec(<<"device/", Name/binary>>, _Opts) -> Name;
mime_to_codec(_, _Opts) -> not_specified.

%% @doc Return the default codec for the given options.
default_codec(Opts) ->
    hb_opts:get(default_codec, <<"httpsig@1.0">>, Opts).

%% @doc Call the `content-type' key on a message with the given codec, using
%% a fast-path for options that are not needed for this one-time lookup.
codec_to_content_type(Codec, Opts) ->
    FastOpts =
        Opts#{
            hashpath => ignore,
            cache_control => [<<"no-cache">>, <<"no-store">>],
            cache_lookup_hueristics => false,
            load_remote_devices => false,
            error_strategy => continue
        },
    case hb_ao:get(<<"content-type">>, #{ <<"device">> => Codec }, FastOpts) of
        not_found -> undefined;
        CT -> CT
    end.

%% @doc Convert a cowboy request to a normalized message.
req_to_tabm_singleton(Req, Body, Opts) ->
    case cowboy_req:header(<<"codec-device">>, Req, <<"httpsig@1.0">>) of
        <<"httpsig@1.0">> ->
			?event({req_to_tabm_singleton, {request, {explicit, Req}, {body, {string, Body}}}}),
            httpsig_to_tabm_singleton(Req, Body, Opts);
        <<"ans104@1.0">> ->
            Item = ar_bundles:deserialize(Body),
            ?event(ans104,
                {deserialized_ans104,
                    {item, Item},
                    {exact, {explicit, Item}}
                }
            ),
            case ar_bundles:verify_item(Item) of
                true ->
                    ?event(ans104, {valid_ans104_signature, Item}),
                    ANS104 =
                        hb_message:convert(
                            Item,
                            <<"structured@1.0">>,
                            <<"ans104@1.0">>,
                            Opts
                        ),
                    normalize_unsigned(Req, ANS104, Opts);
                false ->
                    throw({invalid_ans104_signature, Item})
            end;
        Codec ->
            % Assume that the codec stores the encoded message in the `body' field.
            Decoded =
                hb_message:convert(
                    Body,
                    <<"structured@1.0">>,
                    Codec,
                    Opts
                ),
            ?event(debug,
                {verifying_encoded_message,
                    {body, {string, Body}},
                    {decoded, Decoded}
                }
            ),
            case hb_message:verify(Decoded, all) of
                true ->
                    normalize_unsigned(Req, Decoded, Opts);
                false ->
                    throw({invalid_signature, Decoded})
            end
    end.

%% @doc HTTPSig messages are inherently mixed into the transport layer, so they
%% require special handling in order to be converted to a normalized message.
%% In particular, the signatures are verified if present and required by the 
%% node configuration. Additionally, non-committed fields are removed from the
%% message if it is signed, with the exception of the `path' and `method' fields.
httpsig_to_tabm_singleton(Req = #{ headers := RawHeaders }, Body, Opts) ->
    {ok, SignedMsg} =
        hb_message:with_only_committed(
            hb_message:convert(
                RawHeaders#{ <<"body">> => Body },
                <<"structured@1.0">>,
                <<"httpsig@1.0">>,
                Opts
            ),
            Opts
        ),
    ForceSignedRequests = hb_opts:get(force_signed_requests, false, Opts),
    case (not ForceSignedRequests) orelse hb_message:verify(SignedMsg, all, Opts) of
        true ->
            ?event(http_verify, {verified_signature, SignedMsg}),
            Signers = hb_message:signers(SignedMsg, Opts),
            case Signers =/= [] andalso hb_opts:get(store_all_signed, false, Opts) of
                true ->
                    ?event(http_verify, {storing_signed_from_wire, SignedMsg}),
					Store = maps:get(store, Opts, #{
                                        <<"store-module">> => hb_store_fs,
                                        <<"prefix">> => <<"cache-http">>
                                    }),
                    {ok, _} = hb_cache:write(SignedMsg, Opts#{ store => Store });
                false ->
                    do_nothing
            end,
            normalize_unsigned(Req, SignedMsg, Opts);
        false ->
            ?event(http_verify,
                {invalid_signature,
                    {raw, RawHeaders},
                    {signed, SignedMsg},
                    {force, ForceSignedRequests}
                }
            ),
            throw({invalid_signature, SignedMsg})
    end.

%% @doc Add the method and path to a message, if they are not already present.
%% Remove browser-added fields that are unhelpful during processing (for example,
%% `content-length').
%% The precidence order for finding the path is:
%% 1. The path in the message
%% 2. The path in the request URI
normalize_unsigned(Req = #{ headers := RawHeaders }, Msg, Opts) ->
    Method = cowboy_req:method(Req),
    MsgPath =
        hb_ao:get(
            <<"path">>,
            Msg,
            hb_maps:get(
                <<"path">>, 
                RawHeaders,
                iolist_to_binary(
                    cowboy_req:uri(
                        Req,
                        #{
                            host => undefined,
                            port => undefined,
                            scheme => undefined
                        }
                    )
                ),
				Opts
            ),
            Opts
        ),
    (remove_unless_signed([<<"content-length">>], Msg, Opts))#{
        <<"method">> => Method,
        <<"path">> => MsgPath
    }.

%% @doc Remove all keys from the message unless they are signed.
remove_unless_signed(Key, Msg, Opts) when not is_list(Key) ->
    remove_unless_signed([Key], Msg, Opts);
remove_unless_signed(Keys, Msg, Opts) ->
    SignedKeys = hb_message:committed(Msg, all, Opts),
    maps:without(
        lists:filter(fun(K) -> not lists:member(K, SignedKeys) end, Keys),
        Msg
    ).

%%% Tests

simple_ao_resolve_unsigned_test() ->
    URL = hb_http_server:start_node(),
    TestMsg = #{ <<"path">> => <<"/key1">>, <<"key1">> => <<"Value1">> },
    ?assertEqual({ok, <<"Value1">>}, post(URL, TestMsg, #{})).

simple_ao_resolve_signed_test() ->
    URL = hb_http_server:start_node(),
    TestMsg = #{ <<"path">> => <<"/key1">>, <<"key1">> => <<"Value1">> },
    Wallet = hb:wallet(),
    {ok, Res} =
        post(
            URL,
            hb_message:commit(TestMsg, Wallet),
            #{}
        ),
    ?assertEqual(<<"Value1">>, Res).

nested_ao_resolve_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(Opts),
    Wallet = hb:wallet(),
    {ok, Res} =
        post(
            URL,
            hb_message:commit(#{
                <<"path">> => <<"/key1/key2/key3">>,
                <<"key1">> =>
                    #{<<"key2">> =>
                        #{
                            <<"key3">> => <<"Value2">>
                        }
                    }
            }, Opts#{ priv_wallet => Wallet }),
			Opts
        ),
    ?assertEqual(<<"Value2">>, Res).

wasm_compute_request(Opts, ImageFile, Func, Params) ->
    wasm_compute_request(Opts, ImageFile, Func, Params, <<"">>).
wasm_compute_request(Opts, ImageFile, Func, Params, ResultPath) ->
    {ok, Bin} = file:read_file(ImageFile),
    Wallet = hb:wallet(),
    hb_message:commit(#{
        <<"path">> => <<"/init/compute/results", ResultPath/binary>>,
        <<"device">> => <<"WASM-64@1.0">>,
        <<"function">> => Func,
        <<"parameters">> => Params,
        <<"body">> => Bin
    }, Opts#{ priv_wallet => Wallet}).

run_wasm_unsigned_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    Node = hb_http_server:start_node(Opts#{force_signed => false}),
    Msg = wasm_compute_request(Opts, <<"test/test-64.wasm">>, <<"fac">>, [3.0]),
    {ok, Res} = post(Node, Msg, Opts),
    ?assertEqual(6.0, hb_ao:get(<<"output/1">>, Res, Opts)).

run_wasm_signed_test() ->
 	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(Opts#{force_signed => true}),
    Msg = wasm_compute_request(Opts, <<"test/test-64.wasm">>, <<"fac">>, [3.0], <<"">>),
    {ok, Res} = post(URL, Msg, Opts),
    ?assertEqual(6.0, hb_ao:get(<<"output/1">>, Res, Opts)).

get_deep_unsigned_wasm_state_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(Opts#{force_signed => false}),
    Msg = wasm_compute_request(Opts, <<"test/test-64.wasm">>, <<"fac">>, [3.0], <<"">>),
    {ok, Res} = post(URL, Msg, Opts),
    ?assertEqual(6.0, hb_ao:get(<<"/output/1">>, Res, Opts)).

get_deep_signed_wasm_state_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(Opts#{force_signed => true}),
    Msg = wasm_compute_request(Opts,
        <<"test/test-64.wasm">>, <<"fac">>, [3.0], <<"/output">>),
    {ok, Res} = post(URL, Msg, Opts),
    ?assertEqual(6.0, hb_ao:get(<<"1">>, Res, Opts)).

cors_get_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(),
    {ok, Res} = get(URL, <<"/~meta@1.0/info">>, Opts),
    ?assertEqual(
        <<"*">>,
        hb_ao:get(<<"access-control-allow-origin">>, Res, Opts)
    ).

ans104_wasm_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    URL = hb_http_server:start_node(Opts#{force_signed => true}),
    {ok, Bin} = file:read_file(<<"test/test-64.wasm">>),
    Wallet = hb:wallet(),
    Msg = hb_message:commit(#{
        <<"path">> => <<"/init/compute/results">>,
        <<"accept-codec">> => <<"ans104@1.0">>,
        <<"codec-device">> => <<"ans104@1.0">>,
        <<"device">> => <<"WASM-64@1.0">>,
        <<"function">> => <<"fac">>,
        <<"parameters">> => [3.0],
        <<"body">> => Bin
    }, Opts#{ priv_wallet => Wallet}, <<"ans104@1.0">>),
    ?event({msg, Msg}),
    {ok, Res} = post(URL, Msg, Opts),
    ?event({res, Res}),
    ?assertEqual(6.0, hb_ao:get(<<"output/1">>, Res, Opts)).

send_large_signed_request_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    % Note: If the signature scheme ever changes, we will need to do
    % `hb_message:commit(hb_message:uncommitted(Req), #{})' to get a freshly
    % signed request.
    {ok, [Req]} = file:consult(<<"test/large-message.eterm">>),
    % Get the short trace length from the node message in the large, stored
    % request. 
    ?event({request_message, Req}),
    ?assertMatch(
        {ok, 5},
        post(
            hb_http_server:start_node(Opts),
            <<"/node-message/short_trace_len">>,
            Req,
            #{ http_client => httpc }
        )
    ).

index_test() ->
	Opts = #{
		 store => #{<<"store-module">> => hb_store_fs, <<"prefix">> => <<"cache-TEST">>}
	},
    NodeURL = hb_http_server:start_node(),
    {ok, Res} = get(NodeURL, <<"/~test-device@1.0/load">>, Opts),
    ?assertEqual(<<"i like turtles!">>, hb_ao:get(<<"body">>, Res, Opts)).

index_request_test() ->
    URL = hb_http_server:start_node(),
    {ok, Res} = get(URL, <<"/~test-device@1.0/load?name=dogs">>, #{}),
    ?assertEqual(<<"i like dogs!">>, hb_ao:get(<<"body">>, Res, #{})).

send_encoded_node_message_test(Config, Codec) ->
    NodeURL = hb_http_server:start_node(
        #{
            priv_wallet => ar_wallet:new(),
            operator => <<"unclaimed">>
        }
    ),
    {ok, Res} =
        post(
            NodeURL,
            <<"/~meta@1.0/info">>,
            #{
                <<"codec-device">> => Codec,
                <<"body">> => Config
            },
            #{}
        ),
    ?event(debug, {res, Res}),
    ?assertEqual(
        {ok, <<"b">>},
        hb_http:get(
            NodeURL,
            <<"/~meta@1.0/info/test_optionb">>,
            #{}
        )
    ),
    ?assertEqual(
        {ok, <<"c">>},
        hb_http:get(
            NodeURL,
            <<"/~meta@1.0/info/test_deep/c">>,
            #{}
        )
    ).

send_flat_encoded_node_message_test() ->
    send_encoded_node_message_test(
        <<"test_option: a\ntest_optionb: b\ntest_deep/c: c">>,
        <<"flat@1.0">>
    ).

send_json_encoded_node_message_test() ->
    send_encoded_node_message_test(
        <<
            "{\"test_option\": \"a\", \"test_optionb\": \"b\", \"test_deep\": "
                "{\"c\": \"c\"}}"
        >>,
        <<"json@1.0">>
    ).