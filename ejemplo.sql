SELECT          __id2in ( "s_74_22_t8"."S") AS "item",
                NULL
                /* due to conflict on itemLabel */
                                                        AS "itemLabel",
                __ro2sq ( "s_69_6"."twitter")           AS "twitter",
                __ro2sq ( "s_70_9"."LinkedIN")          AS "LinkedIN",
                __ro2sq ( "s_73_17"."politieke_partij") AS "politieke_partij",
                NULL
                /* due to conflict on politieke_partijLabel */
                                                        AS "politieke_partijLabel",
                __ro2sq ( "s_72_14"."positie_op_lijst") AS "positie_op_lijst"
FROM            db.dba.rdf_quad                         AS "s_74_22_t7"
INNER JOIN      db.dba.rdf_quad                         AS "s_74_22_t8"
ON              (
                                "s_74_22_t8"."S" = "s_74_22_t7"."S")
LEFT OUTER JOIN
                (
                       SELECT "s_69_6_t0"."S" AS "item",
                              "s_69_6_t0"."O" AS "twitter"
                       FROM   db.dba.rdf_quad AS "s_69_6_t0"
                       WHERE  "s_69_6_t0"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/direct/P2002' , 1)) ) AS "s_69_6"
ON              (
                                "s_74_22_t8"."S" = "s_69_6"."item"
                AND             "s_74_22_t7"."S" = "s_69_6"."item")
LEFT OUTER JOIN
                (
                       SELECT "s_70_9_t1"."S" AS "item",
                              "s_70_9_t1"."O" AS "LinkedIN"
                       FROM   db.dba.rdf_quad AS "s_70_9_t1"
                       WHERE  "s_70_9_t1"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/direct/P2035' , 1))) AS "s_70_9"
ON              (
                                "s_74_22_t8"."S" = "s_70_9"."item"
                AND             "s_74_22_t7"."S" = "s_70_9"."item")
INNER JOIN      db.dba.rdf_quad AS "s_74_24_t10"
ON              (
                                "s_74_24_t10"."S" = "s_74_22_t7"."O")
LEFT OUTER JOIN
                (
                       SELECT "s_72_14_t2"."S" AS "node",
                              "s_72_14_t2"."O" AS "positie_op_lijst"
                       FROM   db.dba.rdf_quad  AS "s_72_14_t2"
                       WHERE  "s_72_14_t2"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/qualifier/P1545' , 1))) AS "s_72_14"
ON              (
                                "s_74_24_t10"."S" = "s_72_14"."node"
                AND             "s_74_22_t7"."O" = "s_72_14"."node")
LEFT OUTER JOIN
                (
                       SELECT "s_73_17_t3"."S" AS "node",
                              "s_73_17_t3"."O" AS "politieke_partij"
                       FROM   db.dba.rdf_quad  AS "s_73_17_t3"
                       WHERE  "s_73_17_t3"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/qualifier/P1268' , 1))) AS "s_73_17"
ON              (
                                "s_74_24_t10"."S" = "s_73_17"."node"
                AND             "s_74_22_t7"."O" = "s_73_17"."node")
INNER JOIN      db.dba.sparql_sinv_2 "s_74_20"
ON              (
                                "s_74_20".qtext_template = ' SELECT ?stubvar26\012 WHERE {  <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> "[AUTO_LANGUAGE],nl" . }'
                AND             "s_74_20".qtext_posmap = N''
                AND             "s_74_20".ws_endpoint = __bft ( __bft( 'http://wikiba.se/ontology#label' , 1), 1)
                AND             "s_74_20".ws_params = Vector ()
                AND             "s_74_20".expected_vars = Vector ( 'stubvar26' )
                AND             "s_74_20".param_row = Vector ( NULL
                                /* runaway politieke_partij after reorder */
                                , NULL
                                /* runaway node after reorder */
                                , NULL
                                /* runaway positie_op_lijst after reorder */
                                , NULL
                                /* runaway LinkedIN after reorder */
                                , NULL
                                /* runaway item after reorder */
                                , NULL
                                /* runaway twitter after reorder */
                                ))
WHERE           "s_74_22_t7"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/P3602' , 1))
AND             "s_74_22_t8"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/direct/P551' , 1))
AND             "s_74_22_t8"."O" = __i2idn ( __bft( 'http://www.wikidata.org/entity/Q12892' , 1))
AND             "s_74_24_t10"."P" = __i2idn ( __bft( 'http://www.wikidata.org/prop/statement/P3602' , 1))
AND             "s_74_24_t10"."O" = __i2idn ( __bft( 'http://www.wikidata.org/entity/Q16061881' , 1)) option (quietcast)

