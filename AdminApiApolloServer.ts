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
      check_database_connection(db_type:String, connection:JSON):String 
      database_exists(db_name:String):Boolean
      database_native_tables(db_name:String):JSON
      database(db_name:String):JSON
      database_tables(db_name:String):JSON
  }

  type Mutation {
      save_database(database:JSON):String 
      delete_database(db_name:String):String 
      save_table(table:JSON):String 
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
                },
                check_database_connection: async (parent: any, args: { db_type: string, connection: string }) => {
                    return this.app.schema.checkDatabaseConnection(args.db_type as any, JSON.parse(args.connection));
                },
                database_exists: async (parent: any, args: { db_name: string }) => {
                    return !!this.app.schema.databaseByName[args.db_name];
                },
                database_native_tables: async (parent: any, args: { db_name: string }) => {
                    return this.app.schema.getDatabaseNativeTables(args.db_name);
                },
                database: async (parent: any, args: { db_name: string }) => {
                    return this.app.schema.databaseByName[args.db_name];
                },
                database_tables: async (parent: any, args: { db_name: string }) => {
                    return this.app.schema.info.tables.filter((t) => t.dbname === args.db_name);
                },
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
                },
                save_table: (parent: any, args: { table: string }) => {
                    let table = JSON.parse(args.table);
                    this.app.schema.upsertTable(table);
                    console.log("table", table);
                    this.app.mainApolloServer.restart();
                    return "ok";
                },
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

