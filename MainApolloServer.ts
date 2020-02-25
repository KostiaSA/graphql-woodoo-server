import { Schema } from './schema/schema';
import { ApolloServer, gql } from 'apollo-server';
import { App } from './App';

export class MainApolloServer {
    app: App;
    apolloServer: ApolloServer;

    constructor(_app: App) {
        this.app = _app;
    }

    async start() {
        this.apolloServer = new ApolloServer({
            typeDefs: gql(this.app.schema.createGraphQLSchema()),
            resolvers: this.app.schema.createGraphQLResolvers()
        });
        let result = await this.apolloServer.listen(4000);
        console.log(`graphql-woodoo server ready at ${result.url}`);

    }

    stop() {

    }
}