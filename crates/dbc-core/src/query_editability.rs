use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        Expr, FunctionArguments, GroupByExpr, ObjectName, Select, SelectItem,
        SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, Visit, Visitor,
    },
    dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect},
    parser::Parser,
};

use crate::{
    sql::SqlDialect,
    table_data::{TableKind, TableMetadata, TableRef},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySourceAnalysis {
    pub table: Option<TableRef>,
    pub projections: Vec<QueryProjection>,
    pub wildcard: bool,
    pub reason: Option<QueryEditabilityReason>,
}

impl QuerySourceAnalysis {
    fn read_only(reason: QueryEditabilityReason) -> Self {
        Self {
            table: None,
            projections: Vec::new(),
            wildcard: false,
            reason: Some(reason),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProjection {
    pub output_name: Option<String>,
    pub source_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryColumnEditability {
    pub result_column: String,
    pub source_column: Option<String>,
    pub editable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryEditability {
    pub editable: bool,
    pub columns: Vec<QueryColumnEditability>,
    pub allow_insert: bool,
    pub allow_delete: bool,
    pub reason: Option<QueryEditabilityReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryEditabilityReason {
    InvalidSql,
    MultipleStatements,
    NotAQuery,
    Cte,
    SetOperation,
    MultipleSources,
    DerivedSource,
    Distinct,
    Aggregation,
    UnsupportedSelect,
    TableMismatch,
    View,
    NoStableKey,
    PrimaryKeyNotReturned,
}

#[must_use]
pub fn analyze_query_source(sql: &str, dialect: SqlDialect) -> QuerySourceAnalysis {
    let dialect: &dyn Dialect = match dialect {
        SqlDialect::Generic => &GenericDialect {},
        SqlDialect::PostgreSql => &PostgreSqlDialect {},
        SqlDialect::MySql => &MySqlDialect {},
        SqlDialect::SQLite => &SQLiteDialect {},
    };
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::InvalidSql);
    };
    if statements.len() != 1 {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::MultipleStatements);
    }
    let Statement::Query(query) = &statements[0] else {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::NotAQuery);
    };
    if query.with.is_some() {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::Cte);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::SetOperation);
    };
    analyze_select(select)
}

fn analyze_select(select: &Select) -> QuerySourceAnalysis {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::MultipleSources);
    }
    if select.distinct.is_some() {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::Distinct);
    }
    if has_grouping(select) || select.having.is_some() || contains_aggregate(select) {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::Aggregation);
    }
    if select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::UnsupportedSelect);
    }

    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = &select.from[0].relation
    else {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::DerivedSource);
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::UnsupportedSelect);
    }

    let Some(table) = table_ref(name) else {
        return QuerySourceAnalysis::read_only(QueryEditabilityReason::DerivedSource);
    };
    let qualifier = alias
        .as_ref()
        .map_or_else(|| table.name.as_str(), |alias| alias.name.value.as_str());
    let mut wildcard = false;
    let projections = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expression) => projection(expression, None, qualifier),
            SelectItem::ExprWithAlias { expr, alias } => {
                projection(expr, Some(alias.value.clone()), qualifier)
            }
            SelectItem::ExprWithAliases { .. } => QueryProjection {
                output_name: None,
                source_column: None,
            },
            SelectItem::Wildcard(_) => {
                wildcard = true;
                QueryProjection {
                    output_name: None,
                    source_column: None,
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                if matches_qualifier(kind, qualifier) {
                    wildcard = true;
                }
                QueryProjection {
                    output_name: None,
                    source_column: None,
                }
            }
        })
        .collect();

    QuerySourceAnalysis {
        table: Some(table),
        projections,
        wildcard,
        reason: None,
    }
}

fn has_grouping(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, modifiers) => {
            !expressions.is_empty() || !modifiers.is_empty()
        }
    }
}

fn contains_aggregate(select: &Select) -> bool {
    struct AggregateVisitor;

    impl Visitor for AggregateVisitor {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            let Expr::Function(function) = expression else {
                return ControlFlow::Continue(());
            };
            let name = function
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|identifier| identifier.value.to_ascii_lowercase());
            let aggregate = name.is_some_and(|name| {
                matches!(
                    name.as_str(),
                    "any_value"
                        | "array_agg"
                        | "arrayagg"
                        | "avg"
                        | "bit_and"
                        | "bit_or"
                        | "bit_xor"
                        | "bool_and"
                        | "bool_or"
                        | "collect"
                        | "corr"
                        | "count"
                        | "covar_pop"
                        | "covar_samp"
                        | "every"
                        | "group_concat"
                        | "json_agg"
                        | "json_arrayagg"
                        | "json_group_array"
                        | "json_group_object"
                        | "json_objectagg"
                        | "listagg"
                        | "max"
                        | "median"
                        | "min"
                        | "mode"
                        | "percentile"
                        | "percentile_cont"
                        | "percentile_disc"
                        | "regr_avgx"
                        | "regr_avgy"
                        | "regr_count"
                        | "regr_intercept"
                        | "regr_r2"
                        | "regr_slope"
                        | "regr_sxx"
                        | "regr_sxy"
                        | "regr_syy"
                        | "std"
                        | "stddev"
                        | "stddev_pop"
                        | "stddev_samp"
                        | "string_agg"
                        | "sum"
                        | "total"
                        | "variance"
                        | "var_pop"
                        | "var_samp"
                        | "xmlagg"
                )
            });
            let aggregate_syntax = function.filter.is_some()
                || matches!(
                    &function.args,
                    FunctionArguments::List(arguments)
                        if arguments.duplicate_treatment.is_some()
                )
                || !function.within_group.is_empty();
            if (aggregate || aggregate_syntax) && function.over.is_none() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let mut visitor = AggregateVisitor;
    select.projection.visit(&mut visitor).is_break()
}

fn table_ref(name: &ObjectName) -> Option<TableRef> {
    let mut parts = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|identifier| identifier.value.clone()))
        .collect::<Option<Vec<_>>>()?;
    let name = parts.pop()?;
    Some(TableRef::new(parts, name))
}

fn projection(expression: &Expr, alias: Option<String>, qualifier: &str) -> QueryProjection {
    let source_column = direct_column(expression, qualifier);
    let output_name = alias.or_else(|| source_column.clone());
    QueryProjection {
        output_name,
        source_column,
    }
}

fn direct_column(expression: &Expr, qualifier: &str) -> Option<String> {
    match expression {
        Expr::Identifier(identifier) => Some(identifier.value.clone()),
        Expr::CompoundIdentifier(identifiers)
            if identifiers.len() >= 2
                && identifiers[identifiers.len() - 2]
                    .value
                    .eq_ignore_ascii_case(qualifier) =>
        {
            identifiers
                .last()
                .map(|identifier| identifier.value.clone())
        }
        _ => None,
    }
}

fn matches_qualifier(kind: &SelectItemQualifiedWildcardKind, qualifier: &str) -> bool {
    let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
        return false;
    };
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|identifier| identifier.value.eq_ignore_ascii_case(qualifier))
}

#[must_use]
pub fn resolve_query_editability(
    analysis: &QuerySourceAnalysis,
    metadata: &TableMetadata,
    result_columns: &[String],
) -> QueryEditability {
    if let Some(reason) = analysis.reason {
        return read_only_result(result_columns, reason);
    }
    if !analysis
        .table
        .as_ref()
        .is_some_and(|table| table_matches(table, &metadata.table))
    {
        return read_only_result(result_columns, QueryEditabilityReason::TableMismatch);
    }
    if metadata.kind != TableKind::Table {
        return read_only_result(result_columns, QueryEditabilityReason::View);
    }
    let Some(key) = metadata.stable_key() else {
        return read_only_result(result_columns, QueryEditabilityReason::NoStableKey);
    };

    let columns = result_columns
        .iter()
        .map(|result_column| {
            let source_column = resolve_source_column(analysis, metadata, result_column);
            let editable = source_column.as_deref().is_some_and(|name| {
                metadata
                    .column(name)
                    .is_some_and(|column| column.writable())
            });
            QueryColumnEditability {
                result_column: result_column.clone(),
                source_column,
                editable,
            }
        })
        .collect::<Vec<_>>();
    let key_returned = key.columns.iter().all(|key_column| {
        columns.iter().any(|column| {
            column
                .source_column
                .as_deref()
                .is_some_and(|source| source == key_column)
        })
    });
    if !key_returned {
        return QueryEditability {
            editable: false,
            columns,
            allow_insert: false,
            allow_delete: false,
            reason: Some(QueryEditabilityReason::PrimaryKeyNotReturned),
        };
    }

    QueryEditability {
        editable: columns.iter().any(|column| column.editable),
        columns,
        allow_insert: analysis.wildcard,
        allow_delete: true,
        reason: None,
    }
}

fn resolve_source_column(
    analysis: &QuerySourceAnalysis,
    metadata: &TableMetadata,
    result_column: &str,
) -> Option<String> {
    let matches = analysis
        .projections
        .iter()
        .filter(|projection| {
            projection
                .output_name
                .as_deref()
                .is_some_and(|name| name == result_column)
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        return matches[0].source_column.clone();
    }
    if matches.is_empty() && analysis.wildcard && metadata.column(result_column).is_some() {
        return Some(result_column.to_owned());
    }
    None
}

fn table_matches(left: &TableRef, right: &TableRef) -> bool {
    left.name.eq_ignore_ascii_case(&right.name)
        && (left.qualifiers.is_empty()
            || right.qualifiers.is_empty()
            || left
                .qualifiers
                .iter()
                .rev()
                .zip(right.qualifiers.iter().rev())
                .all(|(left, right)| left.eq_ignore_ascii_case(right)))
}

fn read_only_result(result_columns: &[String], reason: QueryEditabilityReason) -> QueryEditability {
    QueryEditability {
        editable: false,
        columns: result_columns
            .iter()
            .map(|column| QueryColumnEditability {
                result_column: column.clone(),
                source_column: None,
                editable: false,
            })
            .collect(),
        allow_insert: false,
        allow_delete: false,
        reason: Some(reason),
    }
}
