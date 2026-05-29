import {
  createIndexStatement,
  createTableStatement,
  type DiffedIndex,
  type Schema,
  type SchemaDiff,
  type SchemaTables,
  type TableDefinition,
  type TableIndex,
} from "./types";

function indexFieldsEqual(left: TableIndex, right: TableIndex): boolean {
  return (
    left.fields.length === right.fields.length &&
    left.fields.every((field, position) => field === right.fields[position])
  );
}

function createSchemaDiff(options: {
  addedTables: SchemaTables;
  orphanedTables: string[];
  addedIndexes: readonly DiffedIndex[];
  removedIndexes: readonly DiffedIndex[];
}): SchemaDiff {
  const hasChanges =
    Object.keys(options.addedTables).length > 0 ||
    options.orphanedTables.length > 0 ||
    options.addedIndexes.length > 0 ||
    options.removedIndexes.length > 0;

  return {
    addedTables: options.addedTables,
    addedIndexes: options.addedIndexes,
    removedIndexes: options.removedIndexes,
    orphanedTables: options.orphanedTables,
    hasChanges,
    toStatements(): string[] {
      const statements: string[] = [];

      // Orphaned tables are intentionally NOT dropped here — removing a table
      // from the schema must never destroy data. They are reported via
      // `orphanedTables` and deleted explicitly through the dashboard.
      for (const removedIndex of options.removedIndexes) {
        statements.push(
          `DROP INDEX IF EXISTS ${removedIndex.tableName}_${removedIndex.index.name}`
        );
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
  addedTables: SchemaTables,
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

function diffIndexes(
  tableName: string,
  current: TableDefinition,
  target: TableDefinition,
  addedIndexes: DiffedIndex[],
  removedIndexes: DiffedIndex[]
): void {
  const currentIndexes = new Map(
    current.indexes.map((index) => [index.name, index])
  );
  const targetIndexes = new Map(
    target.indexes.map((index) => [index.name, index])
  );

  for (const [indexName, index] of targetIndexes) {
    const existing = currentIndexes.get(indexName);
    if (!existing) {
      addedIndexes.push({ tableName, index });
      continue;
    }

    // Same name, different fields → recreate (DROP old + CREATE new).
    if (!indexFieldsEqual(existing, index)) {
      removedIndexes.push({ tableName, index: existing });
      addedIndexes.push({ tableName, index });
    }
  }

  for (const [indexName, index] of currentIndexes) {
    if (!targetIndexes.has(indexName)) {
      removedIndexes.push({ tableName, index });
    }
  }
}

function collectTableUpdates(
  current: Schema,
  target: Schema,
  orphanedTables: string[],
  addedIndexes: DiffedIndex[],
  removedIndexes: DiffedIndex[]
): void {
  for (const [tableName, table] of Object.entries(current.tables)) {
    const targetTable = target.tables[tableName];

    if (!targetTable) {
      orphanedTables.push(tableName);
      continue;
    }

    diffIndexes(tableName, table, targetTable, addedIndexes, removedIndexes);
  }
}

export function diff(current: Schema, target: Schema): SchemaDiff {
  const addedTables: SchemaTables = {};
  const orphanedTables: string[] = [];
  const addedIndexes: DiffedIndex[] = [];
  const removedIndexes: DiffedIndex[] = [];

  collectNewTableChanges(current, target, addedTables, addedIndexes);
  collectTableUpdates(
    current,
    target,
    orphanedTables,
    addedIndexes,
    removedIndexes
  );

  return createSchemaDiff({
    addedTables,
    orphanedTables,
    addedIndexes,
    removedIndexes,
  });
}
