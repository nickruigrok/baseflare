export type PermissionDecision = boolean | Promise<boolean>;

export type RuleOperation = "read" | "insert" | "update" | "delete";

export interface ReadRuleInput<
  TContext = unknown,
  TDocument = Record<string, unknown>,
> {
  ctx: TContext;
  doc: TDocument;
}

export interface InsertRuleInput<
  TContext = unknown,
  TValue = Record<string, unknown>,
> {
  ctx: TContext;
  value: TValue;
}

export interface UpdateRuleInput<
  TContext = unknown,
  TDocument = Record<string, unknown>,
  TValue = Record<string, unknown>,
> {
  ctx: TContext;
  existingDoc: TDocument;
  value: TValue;
}

export interface DeleteRuleInput<
  TContext = unknown,
  TDocument = Record<string, unknown>,
> {
  ctx: TContext;
  existingDoc: TDocument;
}

export interface TableRules<
  TContext = unknown,
  TDocument = Record<string, unknown>,
  TValue = Record<string, unknown>,
> {
  delete?: (input: DeleteRuleInput<TContext, TDocument>) => PermissionDecision;
  insert?: (input: InsertRuleInput<TContext, TValue>) => PermissionDecision;
  read?: (input: ReadRuleInput<TContext, TDocument>) => PermissionDecision;
  update?: (
    input: UpdateRuleInput<TContext, TDocument, TValue>
  ) => PermissionDecision;
}

export type Rules = Record<
  string,
  TableRules<unknown, Record<string, unknown>, Record<string, unknown>>
>;

export type EvaluationInput =
  | ({ tableName: string; operation: "read" } & ReadRuleInput)
  | ({ tableName: string; operation: "insert" } & InsertRuleInput)
  | ({ tableName: string; operation: "update" } & UpdateRuleInput)
  | ({ tableName: string; operation: "delete" } & DeleteRuleInput);
