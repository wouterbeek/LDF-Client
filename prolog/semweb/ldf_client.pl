:- module(
  ldf_client,
  [
    ldf/4,         % +Endpoint, ?S, ?P, ?O
    ldf_estimate/5 % +Endpoint, ?S, ?P, ?O, -NumTriples
  ]
).

/** <module> Linked Data Fragments (LDF) client

@author Wouter Beek
@version 2017/11
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(http/http_open)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/turtle)).
:- use_module(library(uri)).

:- rdf_meta
   ldf(+, r, r, o),
   ldf_estimate(+, r, r, o, -).

:- rdf_register_prefix(hydra, 'http://www.w3.org/ns/hydra/core#').

:- thread_local
    data/3,
    meta/2.





%! ldf(+Endpoint:uri, ?S:rdf_node, ?P:iri, ?O:rdf_term) is nondet.

ldf(Endpoint, _, _, _) :-
  var(Endpoint), !,
  instantiation_error(Endpoint).
ldf(Endpoint, S, P, O) :-
  ldf_request_uri(Endpoint, S, P, O, Uri),
  ldf_request(Uri, S, P, O).



%! ldf_estimate(+Endpoint:uri, ?S:rdf_node, ?P:iri, ?O:rdf_term,
%!              -NumTriples:nonneg) is det.

ldf_estimate(Endpoint, S, P, O, NumTriples) :-
  ldf_request_uri(Endpoint, S, P, O, Uri),
  ldf_http_open(Uri, In),
  call_cleanup(
    rdf_process_turtle(In, assert_quads, [anon_prefix('_:'),format(trig)]),
    close(In)
  ),
  rdf_equal(Q, hydra:totalItems),
  call_cleanup(
    once(meta(Q, literal(type(_,Lex)))),
    retractall(meta(_,_))
  ),
  atom_number(Lex, NumTriples).

assert_quads(Tuples, _) :-
  maplist(assert_quad, Tuples).

assert_quad(rdf(_,_,_)).
assert_quad(rdf(_,P,O,_)) :-
  assert(meta(P,O)).



%! ldf_parameter(+Key:atom, +Value, -Parameter:compound) is det.

ldf_parameter(_, Var, _) :-
  var(Var), !.
ldf_parameter(Key, Literal, Key=Value) :-
  rdf_is_literal(Literal), !,
  (   Literal = literal(ltag(LTag,Lex))
  ->  format(atom(Value), '"~a"@~a', [Lex,LTag])
  ;   Literal = literal(type(D,Lex))
  ->  format(atom(Value), '"~a"^^~a', [Lex,D])
  ).
ldf_parameter(Key, Value, Key=Value).



%! ldf_request(+Uri, ?S, ?P, ?O) is det.

ldf_request(Uri1, S, P, O) :-
  ldf_http_open(Uri1, In),
  call_cleanup(
    rdf_process_turtle(In, assert_tuples, [anon_prefix('_:'),format(trig)]),
    close(In)
  ),
  (   data(S, P, O)
  ;   retractall(data(_,_,_)),
      % Check whether there is a next page with more results.
      rdf_equal(hydra:next, Q),
      once(meta(Q, Uri2)),
      retractall(meta(_,_)),
      ldf_request(Uri2, S, P, O)
  ).

assert_tuples(Tuples, _) :-
  maplist(assert_tuple, Tuples).

assert_tuple(rdf(S,P,O)) :-
  assert(data(S,P,O)).
assert_tuple(rdf(_,P,O,_)) :-
  assert(meta(P,O)).



%! ldf_request_uri(+Endpoint:uri, ?S:rdf_node, ?P:iri, ?O:rdf_term,
%!                 -Uri:uri) is det.
%
% An URI that implements a Linked Triple Fragments request.

ldf_request_uri(Endpoint, S, P, O, Uri) :-
  uri_components(Endpoint, uri_components(Scheme,Auth,Path,_,_)),
  maplist(
    ldf_parameter,
    [subject,predicate,object],
    [S,P,O],
    [SParam,PParam,OParam]
  ),
  include(ground, [SParam,PParam,OParam], QueryComps),
  uri_query_components(Query, [page_size(100)|QueryComps]),
  uri_components(Uri, uri_components(Scheme,Auth,Path,Query,_)).





% HELPERS %

%! ldf_http_open(+Uri:atom, -In:stream) is det.

ldf_http_open(Uri, In) :-
  http_open(Uri, In, [request_header('Accept'='application/trig'),
                      status_code(Status)]),
  (   Status =:= 200
  ->  true
  ;   Status =:= 400
  ->  fail
  ;   domain_error(oneof([200,400]), Status)
  ).
