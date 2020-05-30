/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 *
 * Created by mengjie on 5/29/20.
 * Just print vertices and edge
 */
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "base/Base.h"
#include "fs/TempDir.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/TestUtils.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

static TagID player_tagID = 3001;
static TagID follow_edgeType = 101;
static const std::vector<std::pair<std::string, nebula::cpp2::SupportedType>> player_tag_name_type =
    {{"name", nebula::cpp2::SupportedType::STRING}, {"age", nebula::cpp2::SupportedType::INT}};
static const std::vector<std::pair<std::string, nebula::cpp2::SupportedType>>
    follow_edge_name_type = {{"degree", nebula::cpp2::SupportedType::INT}};

static std::unique_ptr<meta::IndexManager> mockIndexMan(
    TagID tagId,
    const std::vector<std::pair<std::string, nebula::cpp2::SupportedType>>& tag_field_type,
    EdgeType edgeType,
    const std::vector<std::pair<std::string, nebula::cpp2::SupportedType>>& edge_field_type) {
    auto* indexMan = new AdHocIndexManager();
    std::vector<nebula::cpp2::ColumnDef> columns;
    for (auto& item : tag_field_type) {
        nebula::cpp2::ColumnDef column;
        column.name = item.first;
        column.type.type = item.second;
        columns.emplace_back(std::move(column));
    }
    indexMan->addTagIndex(0, tagId + 1000, tagId, std::move(columns));

    columns.clear();
    for (auto& item : edge_field_type) {
        nebula::cpp2::ColumnDef column;
        column.name = item.first;
        column.type.type = item.second;
        columns.emplace_back(std::move(column));
    }
    indexMan->addEdgeIndex(0, edgeType + 100, edgeType, std::move(columns));

    std::unique_ptr<meta::IndexManager> im(indexMan);
    return im;
}

static std::unique_ptr<meta::IndexManager> genNbaIndexMan() {
    return mockIndexMan(player_tagID, player_tag_name_type, follow_edgeType, follow_edge_name_type);
}

static std::shared_ptr<meta::SchemaProviderIf> genTagSchemaProvider(
    const std::vector<std::pair<std::string, nebula::cpp2::SupportedType>>& field_type) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    for (auto& item : field_type) {
        nebula::cpp2::ColumnDef column;
        column.name = item.first;
        column.type.type = item.second;
        schema->addField(column.name, std::move(column.type));
    }
    return schema;
}

static std::unique_ptr<AdHocSchemaManager> genNbaSchemaMan(GraphSpaceID spaceId = 0) {
    auto schemaMan = std::make_unique<AdHocSchemaManager>();
    schemaMan->addTagSchema(spaceId, player_tagID, genTagSchemaProvider(player_tag_name_type));
    schemaMan->addEdgeSchema(spaceId, follow_edgeType, genTagSchemaProvider(follow_edge_name_type));
    return schemaMan;
}

static cpp2::Vertex genPlayerVertex(const VertexID& vId,
                                    const std::string& name,
                                    const int32_t& age) {
    cpp2::Vertex v;
    v.set_id(vId);
    std::vector<cpp2::Tag> tags;
    cpp2::Tag t;
    RowWriter writer(genTagSchemaProvider(player_tag_name_type));
    writer << name << age;
    t.set_props(writer.encode());
    tags.push_back(std::move(t));
    v.set_tags(std::move(tags));

    return v;
}

static cpp2::Edge genFollowEdge(VertexID srcFrom,
                                VertexID srcTo,
                                EdgeType edgeType,
                                int32_t degree) {
    cpp2::Edge edge;
    cpp2::EdgeKey key;
    key.set_src(srcFrom);
    key.set_edge_type(edgeType);
    key.set_dst(srcTo);
    edge.set_key(key);
    RowWriter writer(genTagSchemaProvider(follow_edge_name_type));
    writer << degree;
    edge.set_props(writer.encode());
    return edge;
}

static std::string getPrintString(std::shared_ptr<const meta::SchemaProviderIf> provider,
                                  RowReader* reader) {
    auto numFields = provider->getNumFields();
    std::string printString = "";
    for (size_t i = 0; i < numFields; i++) {
        const auto name = std::string(provider->getFieldName(i));
        const auto& ftype = provider->getFieldType(i);
        switch (ftype.type) {
            case nebula::cpp2::SupportedType::INT: {
                int64_t v;
                auto ret = reader->getInt<int64_t>(i, v);
                EXPECT_EQ(ret, ResultType::SUCCEEDED);
                printString.append("name:" + name + " Value:" + std::to_string(v) + "; ");
                break;
            }
            case nebula::cpp2::SupportedType::STRING: {
                folly::StringPiece v;
                auto ret = reader->getString(i, v);
                EXPECT_EQ(ret, ResultType::SUCCEEDED);
                printString.append("name:" + name + " Value:" + v.toString() + "; ");
                break;
            }
            default: {
                LOG(FATAL) << "Should not reach here!";
                break;
            }
        }
    }
    return printString;
}

static std::string getTagRowValueString(meta::SchemaManager* schema,
                                        folly::StringPiece row,
                                        TagID tagId) {
    auto reader = RowReader::getTagPropReader(schema, row, 0, tagId);
    auto provider = schema->getTagSchema(0, tagId);
    return "tagId: " + std::to_string(tagId) + "; " + getPrintString(provider, reader.get());
}

static std::string getEdgeRowValueString(meta::SchemaManager* schema,
                                         folly::StringPiece row,
                                         EdgeType type) {
    auto reader = RowReader::getEdgePropReader(schema, row, 0, type);
    auto provider = schema->getEdgeSchema(0, type);
    return "EdgeType: " + std::to_string(type) + "; " + getPrintString(provider, reader.get());
}

TEST(QuickStartTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QuickStartTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto nbaSchemaMan = genNbaSchemaMan();
    auto indexMan = genNbaIndexMan();
    auto* pAddVerticesProcessor =
        AddVerticesProcessor::instance(kv.get(), nbaSchemaMan.get(), indexMan.get(), nullptr);

    std::vector<cpp2::Vertex> vertices{genPlayerVertex(100, "Tim Duncan", 42),
                                       genPlayerVertex(101, "Tony Parker", 36),
                                       genPlayerVertex(102, "LaMarcus Aldridge", 33)};
    std::vector<cpp2::Edge> edges{genFollowEdge(100, 101, follow_edgeType, 95),
                                  genFollowEdge(100, 102, follow_edgeType, 90),
                                  genFollowEdge(102, 101, follow_edgeType, 75)};
    int numVertices = vertices.size();

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest addVerticesRequest;
    addVerticesRequest.space_id = 0;
    addVerticesRequest.overwritable = true;
    addVerticesRequest.parts.emplace(0, std::move(vertices));
    LOG(INFO) << "Test Quick start: AddVerticesProcessor...";
    auto futAddVertices = pAddVerticesProcessor->getFuture();
    pAddVerticesProcessor->process(addVerticesRequest);
    auto respAddvertices = std::move(futAddVertices).get();
    EXPECT_EQ(0, respAddvertices.result.failed_codes.size());

    LOG(INFO) << "start AddFollowEdgesRequest...";
    auto* processor =
        AddEdgesProcessor::instance(kv.get(), nbaSchemaMan.get(), indexMan.get(), nullptr);
    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest addEdgesRequest;
    addEdgesRequest.space_id = 0;
    addEdgesRequest.overwritable = true;
    // partId => List<Edge>
    // Edge => {EdgeKey, props}
    addEdgesRequest.parts.emplace(0, std::move(edges));

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto futAddEdges = processor->getFuture();
    processor->process(addEdgesRequest);
    auto respAddEdges = std::move(futAddEdges).get();
    EXPECT_EQ(0, respAddEdges.result.failed_codes.size());

    LOG(INFO) << "Check Vertices data in kv store...";
    for (VertexID vertexId = 100; vertexId < 100 + numVertices; vertexId++) {
        auto prefix = NebulaKeyUtils::vertexPrefix(0, vertexId);
        std::unique_ptr<kvstore::KVIterator> iter;
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 0, prefix, &iter));
        LOG(INFO) << "vertexID:" + std::to_string(vertexId) + "; " +
                         getTagRowValueString(nbaSchemaMan.get(), iter->val(), player_tagID);
    }

    LOG(INFO) << "Check edge data in kv store...";
    for (VertexID srcId = 100; srcId < 100 + numVertices; srcId++) {
        auto prefix = NebulaKeyUtils::edgePrefix(0, srcId, follow_edgeType);
        std::unique_ptr<kvstore::KVIterator> iter;
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 0, prefix, &iter));
        while (iter->valid()) {
            LOG(INFO) << "srcID:" + std::to_string(srcId) + "; " +
                             getEdgeRowValueString(
                                 nbaSchemaMan.get(), iter->val(), follow_edgeType);
            iter->next();
        }
    }
}

}   // namespace storage
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
