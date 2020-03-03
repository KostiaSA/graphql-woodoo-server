import { Schema } from './schema/schema';
import { ApolloServer, gql } from 'apollo-server';
import { GraphQLResolveInfo } from 'graphql';
import { Args } from './resolver/SqlResolver';
import { App } from './App';
const { GraphQLJSON, GraphQLJSONObject } = require('graphql-type-json');

export class AdminApiApolloServer {
    app: App;
    apolloServer: ApolloServer;

    constructor(_app: App) {
        this.app = _app;
    }

    getApiTypeDefs() {

        return gql`

  scalar JSON

  type Query {
      schema: JSON
      tables: JSON
      databases: JSON
  }

  type Mutation {
      save_database(database:JSON):String 
      delete_database(db_name:String):String 
  }

`;
    }

    getApiResolvers() {
        return {
            JSON: GraphQLJSON,
            Query: {
                schema: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
                    return this.app.schema.info;
                },
                tables: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
                    return this.app.schema.info.tables;
                },
                databases: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
                    return this.app.schema.info.databases;
                }
            },

            Mutation: {
                save_database: (parent: any, args: { database: string }) => {
                    let db = JSON.parse(args.database);
                    this.app.schema.upsertDatabase(db);
                    console.log("db", db);
                    this.app.mainApolloServer.restart();
                    return "ok";
                },
                delete_database: (parent: any, args: { db_name: string }) => {
                    this.app.schema.deleteDatabase(args.db_name);
                    this.app.mainApolloServer.restart();
                    return "ok";
                }
            }

        };

    }

    async start() {
        this.apolloServer = new ApolloServer({
            typeDefs: this.getApiTypeDefs(),
            resolvers: this.getApiResolvers()
        });
        let result = await this.apolloServer.listen(3001);
        console.log(`graphql-woodoo server ready at ${result.url}`);

    }

    stop() {

    }
}