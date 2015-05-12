// from knex
// Copyright (c) 2013-2014 Tim Griesser
// All properties we can use to start a query chain
// from the `knex` object, e.g. `knex.select('*').from(...`
'use strict';

module.exports = ['select', 'as', 'columns', 'column', 'from', 'fromJS', 'into', 'table', 'distinct', 'join', 'joinRaw', 'innerJoin', 'leftJoin', 'leftOuterJoin', 'rightJoin', 'rightOuterJoin', 'outerJoin', 'fullOuterJoin', 'crossJoin', 'where', 'andWhere', 'orWhere', 'whereNot', 'orWhereNot', 'whereRaw', 'whereWrapped', 'havingWrapped', 'orWhereRaw', 'whereExists', 'orWhereExists', 'whereNotExists', 'orWhereNotExists', 'whereIn', 'orWhereIn', 'whereNotIn', 'orWhereNotIn', 'whereNull', 'orWhereNull', 'whereNotNull', 'orWhereNotNull', 'whereBetween', 'whereNotBetween', 'orWhereBetween', 'orWhereNotBetween', 'groupBy', 'groupByRaw', 'orderBy', 'orderByRaw', 'union', 'unionAll', 'having', 'havingRaw', 'orHaving', 'orHavingRaw', 'offset', 'limit', 'count', 'min', 'max', 'sum', 'avg', 'increment', 'decrement', 'first', 'debug', 'pluck', 'insert', 'update', 'returning', 'del', 'delete', 'truncate', 'transacting', 'connection'];
