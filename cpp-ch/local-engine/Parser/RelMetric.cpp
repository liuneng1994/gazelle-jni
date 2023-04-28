#include <Processors/IProcessor.h>
#include "RelMetric.h"

using namespace rapidjson;

namespace local_engine
{
RelMetric::RelMetric(size_t id_, String name_, std::vector<DB::IQueryPlanStep *>& steps_) : id(id_), name(name_), steps(steps_)
{
}
RelMetric::RelMetric(String name_, const std::vector<RelMetricPtr> & inputs_, std::vector<DB::IQueryPlanStep *>& steps_) : name(name_), steps(steps_), inputs(inputs_)
{
    auto rel = std::max_element(inputs.begin(), inputs.end(), [](RelMetricPtr a, RelMetricPtr b) {return a->id < b->id;});
    id = rel->get()->id + 1;
}
size_t RelMetric::getId() const
{
    return id;
}
const std::vector<DB::IQueryPlanStep *> & RelMetric::getSteps() const
{
    return steps;
}
const std::vector<RelMetricPtr> & RelMetric::getInputs() const
{
    return inputs;
}
size_t RelMetric::getTotalTime() const
{
    size_t time = 0;
    if (!steps.empty())
    {
        for (const auto * step : steps)
        {
            if (!step->getProcessors().empty())
            {
                for (const auto & processor : step->getProcessors())
                {
                    time += processor->getElapsedUs();
                }
            }
        }
    }
    return time;
}

void RelMetric::serialize(Writer<StringBuffer> & writer, bool summary) const
{
    writer.StartObject();
    writer.String("id");
    writer.Uint(id);
    writer.String("name");
    writer.String(name.c_str());
    writer.String("time");
    writer.Uint(getTotalTime());
    writer.EndObject();
}
const String & RelMetric::getName() const
{
    return name;
}

std::string RelMetricSerializer::serializeRelMetric(RelMetricPtr rel_metric, bool flatten)
{
    StringBuffer result;
    Writer<StringBuffer> writer(result);
    if (flatten)
    {
        writer.StartArray();
        std::stack<RelMetricPtr> metrics;
        metrics.push(rel_metric);
        while (!metrics.empty())
        {
            auto metric = metrics.top();
            metrics.pop();
            for (const auto & item : metric->getInputs())
            {
                metrics.push(item);
            }
            metric->serialize(writer);
        }
        writer.EndArray();
    }
    return result.GetString();
}
}
