import { MainApolloServer } from './MainApolloServer';
import { Schema } from './schema/schema';
import { AdminApiApolloServer } from './AdminApiApolloServer';

export class App {

    schema: Schema;
    mainApolloServer: MainApolloServer;
    adminApiApolloServer: AdminApiApolloServer;

    async start() {
        this.schema = new Schema();

        this.mainApolloServer = new MainApolloServer(this);
        await this.mainApolloServer.start();

        this.adminApiApolloServer = new AdminApiApolloServer(this);
        await this.adminApiApolloServer.start();
    }
}