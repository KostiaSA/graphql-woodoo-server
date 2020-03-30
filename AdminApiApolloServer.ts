import { Schema } from './schema/schema';
import { ApolloServer, gql } from 'apollo-server';
import { GraphQLResolveInfo } from 'graphql';
import { Args } from './resolver/SqlResolver';
import { App } from './App';
import { ITable, IColumn } from '../voodoo-shared/ISchema';
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
      ping:JSON
      schema: JSON

      tables: JSON
      table(db_name:String, table_schema:String, table_name:String):JSON
      table_ref_tables(db_name:String, table_schema:String, table_name:String): JSON
      column(db_name:String, table_schema:String, table_name:String, column_name:String):JSON

      native_table_columns(db_name:String, table_schema:String, table_name:String):JSON

      databases: JSON
      database(db_name:String):JSON
      check_database_connection(db_type:String, connection:JSON):String 
      database_exists(db_name:String):Boolean
      database_native_tables(db_name:String):JSON

      database_tables(db_name:String):JSON
      translate(non_en_words:JSON):JSON

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
                ping: async () => {
                    return { ping: "Ok" };
                },
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
                table: async (parent: any, args: { db_name: string, table_schema: string, table_name: string }) => {
                    return this.app.schema.info.tables.filter((t) => t.dbname === args.db_name && t.dbo === args.table_schema && t.name === args.table_name)[0];
                },
                table_ref_tables: async (parent: any, args: { db_name: string, table_schema: string, table_name: string }) => {
                    var table = this.app.schema.info.tables.filter((t) => t.dbname === args.db_name && t.dbo === args.table_schema && t.name === args.table_name)[0];

                    let getRefColKey = (col: IColumn) => col.ref_db + ":" + col.ref_schema + ":" + col.ref_table;

                    let keys: { [schema_and_table: string]: boolean } = {};
                    for (let col of table.columns.filter((col) => typeof col.ref_db === "string")) {
                        keys[getRefColKey(col)] = true;
                    }
                    return this.app.schema.info.tables.filter((table: ITable) => keys[table.dbname + ":" + table.dbo + ":" + table.name]);

                },
                column: async (parent: any, args: { db_name: string, table_schema: string, table_name: string, column_name: string }) => {
                    var table = this.app.schema.info.tables.filter((t) => t.dbname === args.db_name && t.dbo === args.table_schema && t.name === args.table_name)[0];
                    return table.columns.filter((col) => col.name === args.column_name)[0];
                },
                native_table_columns: async (parent: any, args: { db_name: string, table_schema: string, table_name: string }) => {
                    return this.app.schema.getDatabaseNativeTableColumns(args.db_name, args.table_schema, args.table_name);
                },
                translate: async (parent: any, args: { non_en_words: string }) => {
                    return await this.app.schema.translate(args.non_en_words);
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
        console.log(`graphql-woodoo server admin API ready at ${result.url}`);

    }

    stop() {

    }
}

