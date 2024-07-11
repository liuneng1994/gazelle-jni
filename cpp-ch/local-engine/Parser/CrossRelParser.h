/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <Parser/RelParser.h>
#include <substrait/algebra.pb.h>

namespace DB
{
class TableJoin;
}

namespace local_engine
{

class StorageJoinFromReadBuffer;


class CrossRelParser : public RelParser
{
public:
    explicit CrossRelParser(SerializedPlanParser * plan_paser_);
    ~CrossRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & sort_rel, std::list<const substrait::Rel *> & rel_stack_) override;

    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack) override;

    const substrait::Rel & getSingleInput(const substrait::Rel & rel) override;

private:
    std::unordered_map<std::string, std::string> & function_mapping;
    ContextPtr & context;
    std::vector<QueryPlanPtr> & extra_plan_holder;


    DB::QueryPlanPtr parseJoin(const substrait::CrossRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right);
    void renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join);
    void addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right);
    void addPostFilter(DB::QueryPlan & query_plan, const substrait::CrossRel & join);
    bool applyJoinFilter(
        DB::TableJoin & table_join, const substrait::CrossRel & join_rel, DB::QueryPlan & left, DB::QueryPlan & right, bool allow_mixed_condition);
};

}
