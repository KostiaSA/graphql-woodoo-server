//var { graphql, buildSchema } = require('graphql');
import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode } from 'graphql';
import { Args, SqlResolver } from './resolver/SqlResolver';

// Construct a schema, using GraphQL schema language
var schema = buildSchema(`


type PodrVid {
    podrvid_id: Int!
    podrvid_name: String!
}

type Podr {
    podr_number: Int!
    podr_name: String!
    podr_description: String!
    vid:PodrVid
}

type Query {
    podrs(p1:String, p2:String): [Podr]
}

`);

// The root provides a resolver function for each API endpoint
var root_resolver = {
    podrs: async (a: Args, b: any, c: GraphQLResolveInfo) => {

        let r = new SqlResolver(a, c);
        await r.resolve_query();

        let xxx = c.fieldName;
        let yyy = c.fieldNodes[0].selectionSet.selections.map((item: any) => item.name.value).join();
        //console.log("select", yyy, "from", xxx);
        debugger
        return 'Hello world12!';
    },
};

let q = `
query Q1 { 
    podrs (p1:"param1_value",p2:"param1_value") {
        podr_number
        podr_name
        vid {
           podrvid_name
        }
    }
}
`;


let ast = parse(q);

// // Run the GraphQL query '{ hello }' and print out the response
// graphql(schema, '{ hello }', root).then((response: any) => {
//     console.log(response);
// });

debugger

async function start() {
    let response = await graphql(schema, q, root_resolver);
    debugger
    console.log(response);
}

start().then();