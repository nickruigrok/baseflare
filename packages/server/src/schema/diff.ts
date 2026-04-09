import {
  createIndexStatement,
  createTableStatement,
  type DiffedIndex,
  type Schema,
  type SchemaDiff,
  type TableDefinition,
} from "./types";

function createSchemaDiff(options: {
  addedTables: Record<string, TableDefinition>;
  removedTables: string[];
  addedIndexes: readonly DiffedIndex[];
  removedIndexes: readonly DiffedIndex[];
}): SchemaDiff {
  const hasChanges =
    Object.keys(options.addedTables).length > 0 ||
    options.removedTables.length > 0 ||
    options.addedIndexes.length > 0 ||
    options.removedIndexes.length > 0;

  return {
    ...options,
    hasChanges,
    toStatements(): string[] {
      const statements: string[] = [];

      for (const removedIndex of options.removedIndexes) {
        statements.push(
          `DROP INDEX IF EXISTS ${removedIndex.tableName}_${removedIndex.index.name}`
        );
      }

      for (const removedTable of options.removedTables) {
        statements.push(`DROP TABLE IF EXISTS ${removedTable}`);
      }

      for (const tableName of Object.keys(options.addedTables)) {
        statements.push(createTableStatement(tableName));
      }

      for (const addedIndex of options.addedIndexes) {
        statements.push(
          createIndexStatement(addedIndex.tableName, addedIndex.index)
        );
      }

      return statements;
    },
  };
}

function collectNewTableChanges(
  current: Schema,
  target: Schema,
  addedTables: Record<string, TableDefinition>,
  addedIndexes: DiffedIndex[]
): void {
  for (const [tableName, table] of Object.entries(target.tables)) {
    if (tableName in current.tables) {
      continue;
    }

    addedTables[tableName] = table;
    for (const index of table.indexes) {
      addedIndexes.push({ tableName, index });
    }
  }
}

function collectTableUpdates(
  current: Schema,
  target: Schema,
  removedTables: string[],
  addedIndexes: DiffedIndex[],
  removedIndexes: DiffedIndex[]
): void {
  for (const [tableName, table] of Object.entries(current.tables)) {
    if (!(tableName in target.tables)) {
      removedTables.push(tableName);
      for (const index of table.indexes) {
        removedIndexes.push({ tableName, index });
      }
      continue;
    }

    const targetTable = target.tables[tableName];
    if (!targetTable) {
      continue;
    }

    const currentIndexes = new Map(
      table.indexes.map((index) => [index.name, index])
    );
    const targetIndexes = new Map(
      targetTable.indexes.map((index) => [index.name, index])
    );

    for (const [indexName, index] of targetIndexes) {
      if (!currentIndexes.has(indexName)) {
        addedIndexes.push({ tableName, index });
      }
    }

    for (const [indexName, index] of currentIndexes) {
      if (!targetIndexes.has(indexName)) {
        removedIndexes.push({ tableName, index });
      }
    }
  }
}

export function diff(current: Schema, target: Schema): SchemaDiff {
  const addedTables: Record<string, TableDefinition> = {};
  const removedTables: string[] = [];
  const addedIndexes: DiffedIndex[] = [];
  const removedIndexes: DiffedIndex[] = [];

  collectNewTableChanges(current, target, addedTables, addedIndexes);
  collectTableUpdates(
    current,
    target,
    removedTables,
    addedIndexes,
    removedIndexes
  );

  return createSchemaDiff({
    addedTables,
    removedTables,
    addedIndexes,
    removedIndexes,
  });
}
