
export class SqlBuilder {
    sql: string[] = [];

    toString(delimiter: string = "", levelPads: string = ""): string {
        return this.sql.map((item) => levelPads + item).join(delimiter);
    }

    add(str: string) {
        this.sql.push(str);
    }

    addLine(str: string) {
        this.sql.push(str + "\n");
    }

    deleteLastChar(charsCount: number = 1) {
        this.sql = [this.sql.join("").slice(0, -charsCount)];
    }

}

export class SqlSelectBuilder {
    fields: SqlBuilder = new SqlBuilder();
    from: SqlBuilder = new SqlBuilder();
    where: SqlBuilder = new SqlBuilder();
    orderby: SqlBuilder = new SqlBuilder();

    toSql(levelPads: string = ""): string {

        let sql: string[] = [];

        sql.push(levelPads + "select  \n");
        sql.push(this.fields.toString(",\n", "    "));
        sql.push("\n");
        sql.push(levelPads + "from\n");
        sql.push(this.from.toString(",\n", "    "));
        sql.push("\n");

        if (this.where.toString().length > 0) {
            sql.push(levelPads + "where\n");
            sql.push(this.where.toString(" AND\n", "    "));
            sql.push("\n");
        }

        if (this.orderby.toString().length > 0) {
            sql.push(levelPads + "    order by\n");
            sql.push(this.orderby.toString(", ", "    "));
            sql.push("\n");
        }

        return sql.join("");
    }

}

