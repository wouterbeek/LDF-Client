:- module(
  ldf_client,
  [
    ldf/4,         % +Endpoint, ?S, ?P, ?O
    ldf_estimate/5 % +Endpoint, ?S, ?P, ?O, -NumTriples
  ]
).

/** <module> Linked Data Fragments (LDF) client

@author Wouter Beek
@version 2017/05-2017/08
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_ext)).
:- use_module(library(settings)).
:- use_module(library(uri/uri_ext)).

:- rdf_meta
   ldf(+, r, r, o),
   ldf_estimate(+, r, r, o, -).





ldf(Endpoint, _, _, _) :-
  var(Endpoint), !,
  instantiation_error(Endpoint).
ldf(Endpoint, S, P, O) :-
  ldf_request_uri(Endpoint, S, P, O, Uri),
  ldf_request(Uri, S, P, O).



%! ldf_estimate(+Endpoint, ?S, ?P, ?O, -NumTriples) is det.

ldf_estimate(Endpoint, S, P1, O, NumTriples) :-
  ldf_request_uri(S, P1, O, Endpoint, Uri),
  rdf_deref_quads(Uri, Quads, [media_type(media(application/trig,[]))]),
  include(ldf_is_meta_quad, Quads, MetaQuads),
  rdf_equal(hydra:totalItems, P2),
  memberchk(rdf(_,P2,NumTriples^^_,_), MetaQuads).



%! ldf_is_meta_graph(+G) is semidet.

ldf_is_meta_graph(G) :-
  uri_components(G, uri_components(_,_,_,_,Frag)),
  Frag == metadata.



%! ldf_is_meta_quad(+Quad) is semidet.

ldf_is_meta_quad(rdf(_,_,_,G)) :-
  ldf_is_meta_graph(G).



%! ldf_parameter(+Key, +Val, -Param) is det.

ldf_parameter(_, Val, _) :-
  var(Val), !.
ldf_parameter(Key, Lit, Key=Val) :-
  rdf_is_literal(Lit), !,
  rdf_literal_lexical_form(Lit, Lex),
  (   rdf_is_language_tagged_string(Lit)
  ->  Lit = _@LTag,
      format(atom(Val), '"~a"@~a', [Lex,LTag])
  ;   rdf_literal_datatype(Lit, D),
      format(atom(Val), '"~a"^^~a', [Lex,D])
  ).
ldf_parameter(Key, Val, Key=Val).



%! ldf_request(+Uri, ?S, ?P, ?O) is det.

ldf_request(Uri1, S, P1, O) :-
  rdf_deref_quads(Uri1, Quads, [media_type(media(application/trig,[]))]),
  partition(ldf_is_meta_quad, Quads, MetaQuads, DataQuads),
  (   member(rdf(S,P1,O,_), DataQuads)
  ;   % Check whether there is a next page with more results.
      once((
        % @compat Look for both the current and the legacy property.
        (rdf_equal(hydra:next, P2) ; rdf_equal(hydra:nextPage, P2)),
        memberchk(rdf(_,P2,Uri2,_), MetaQuads)
      )),
      ldf_request(Uri2, S, P1, O)
  ).



%! ldf_request_uri(+Endpoint, ?S, ?P, ?O, -Uri) is det.
%
% An URI that implements a Linked Triple Fragments request.

ldf_request_uri(S, P, O, Endpoint, Uri) :-
  uri_comps(Endpoint, uri(Scheme,Auth,Segments,_,_)),
  maplist(
    ldf_parameter,
    [subject,predicate,object],
    [S,P,O],
    [SParam,PParam,OParam]
  ),
  include(ground, [SParam,PParam,OParam], QueryComps),
  uri_comps(Uri, uri(Scheme,Auth,Segments,[page_size(100)|QueryComps],_)).