import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode, FieldNode } from 'graphql';
import { SqlSelectBuilder } from './SqlBuilder';

export type Args = { [argName: string]: any };


export class SqlResolver {
    args: Args;
    info: GraphQLResolveInfo;
    sql: SqlSelectBuilder = new SqlSelectBuilder();

    constructor(_args: Args, _info: GraphQLResolveInfo) {
        this.args = _args;
        this.info = _info;
    }

    async resolve(): Promise<any> {

        if (this.info.operation.operation == "query") {
            return this.resolve_query();
        }
        else
            throw "todo:"

    }

    async resolve_query(): Promise<any> {

        let tableName = this.info.fieldName;
        this.sql.from.addLine(tableName);

        for (let field of this.info.fieldNodes[0].selectionSet.selections) {
            if (field.kind == 'Field') {
                let f = field as FieldNode;
                if (f.name.kind == "Name")
                    this.sql.fields.add(f.name.value);
                else
                    throw "todo:"
            }
            else
                throw "todo:"
        }

        console.log("sql", this.sql.toSql());

    }
}