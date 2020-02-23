//var { graphql, buildSchema } = require('graphql');
import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode } from 'graphql';
import { Args, SqlResolver } from './resolver/SqlResolver';
import { Schema } from './schema/schema';

//let express = require('express');
//let jsonGraphqlExpress = require('json-graphql-server').default;

const { ApolloServer, gql } = require('apollo-server');
const { GraphQLJSON, GraphQLJSONObject } = require('graphql-type-json');

// Construct a schema, using GraphQL schema language
// var schema1 = gql`


// type PodrVid {
//     podrvid_id: Int!
//     podrvid_name: String!
// }

// type Podr {
//     podr_number: Int!
//     podr_name: String!
//     podr_description: String!
//     vid:PodrVid
// }

// type Query {
//     podrs(p1:String, p2:String): [Podr]
// }

// `;

// debugger
// The root provides a resolver function for each API endpoint
// var root_resolver = {

//     Query: {
//         podrs: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {

//             let r = new SqlResolver(args, info);
//             await r.resolve_query();

//             // let xxx = c.fieldName;
//             // let yyy = c.fieldNodes[0].selectionSet.selections.map((item: any) => item.name.value).join();
//             // //console.log("select", yyy, "from", xxx);
//             // debugger
//             return [{ podr_number: "1", podr_name: "2222222222", vid: { podrvid_id: "xxx", podrvid_name: "podrvid_name111" } }];
//         }
//     },
// };

// let q = `
// query Q1 { 
//     podrs (p1:"param1_value",p2:"param1_value") {
//         podr_number
//         podr_name
//         vid {
//            podrvid_name
//         }
//     }
// }
// `;


//let ast = parse(q);

// // Run the GraphQL query '{ hello }' and print out the response
// graphql(schema, '{ hello }', root).then((response: any) => {
//     console.log(response);
// });


let s = new Schema();
console.log("==============================================================");
console.log(s.createGraphQLSchema());
console.log("==============================================================");

//let x = gql(s.createGraphQLSchema());

const server = new ApolloServer({ typeDefs: gql(s.createGraphQLSchema()), resolvers: s.createGraphQLResolvers() });



var api_schema = gql`
scalar JSON

type Query {
    schema: JSON
    tables: JSON
    databases: JSON
}
`;

var api_resolver = {
    JSON: GraphQLJSON,
    Query: {
        schema: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
            return s.info;
        },
        tables: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
            return s.info.tables;
        },
        databases: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
            return s.info.databases;
        }
    },
};

const api_server = new ApolloServer({ typeDefs: api_schema, resolvers: api_resolver });



async function start() {

    // console.time('1');
    // let xxx = await s.sqlExecute("бухта-wms", "select top 10 Номер,Название from Организация");
    // console.timeEnd('1');
    // console.time('2');
    // let yyy = await s.sqlExecute("бухта-wms", "select top 10 Номер,Название from Организация");
    // console.timeEnd('2');
    // console.time('3');
    // let zzz = await s.sqlExecute("бухта-wms", "select top 10 Номер,Название from Организация");
    // console.timeEnd('3');

    let p = await server.listen(4000);
    console.log(`graphql-woodoo server ready at ${p.url}`);

    let p2 = await api_server.listen(3001);
    console.log(`graphql-woodoo api server ready at ${p2.url}`);

    // let response = await graphql(schema, q, root_resolver);
    // debugger
    // console.log(response);

    // const app = express();
    // const data = s.info;
    // debugger
    // app.use('/', jsonGraphqlExpress(data));
    // app.listen(4001);
}

start();