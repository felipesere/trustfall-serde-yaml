use std::collections::BTreeMap;
use std::iter::successors;
use std::num::NonZeroUsize;
use std::println;
use std::str::FromStr;
use std::sync::Arc;

use async_graphql_parser::types::Type;
use kdl::KdlDocument;
use trustfall::provider::{
    BasicAdapter, ContextIterator, ContextOutcomeIterator, EdgeParameters, VertexIterator,
};
use trustfall::FieldValue;
use trustfall_core::ir::{
    ContextField, Eid, IREdge, IRQuery, IRQueryComponent, IRVertex, IndexedQuery,
    Output as TFOutput, Vid,
};

#[derive(Debug, Clone)]
struct Vertex(Arc<serde_yaml::Value>);

impl From<&serde_yaml::Value> for Vertex {
    fn from(value: &serde_yaml::Value) -> Self {
        Self(Arc::new(value.clone()))
    }
}

impl trustfall::provider::Typename for Vertex {
    fn typename(&self) -> &'static str {
        match *self.0 {
            serde_yaml::Value::Null => "null",
            serde_yaml::Value::Bool(_) => "bool",
            serde_yaml::Value::Number(_) => "number",
            serde_yaml::Value::String(_) => "string",
            serde_yaml::Value::Sequence(_) => "sequence",
            serde_yaml::Value::Mapping(_) => "mapping",
            serde_yaml::Value::Tagged(_) => "tagged",
        }
    }
}

struct YamlAdapter {
    root: Arc<serde_yaml::Value>,
}

impl<'vertex> BasicAdapter<'vertex> for YamlAdapter {
    type Vertex = Vertex;

    fn resolve_starting_vertices(
        &self,
        _edge_name: &str,
        _parameters: &EdgeParameters,
    ) -> VertexIterator<'vertex, Self::Vertex> {
        Box::new(vec![Vertex(self.root.clone())].into_iter())
    }

    fn resolve_property(
        &self,
        contexts: ContextIterator<'vertex, Self::Vertex>,
        type_name: &str,
        property_name: &str,
    ) -> ContextOutcomeIterator<'vertex, Self::Vertex, FieldValue> {
        let type_name = type_name.to_string();
        let property_name = property_name.to_string();
        Box::new(contexts.filter_map(move |ctx| {
            let node = ctx.active_vertex().clone().unwrap();

            println!("Looking for {property_name} of type {type_name} on {node:?}:");
            node.0
                .get(&property_name)
                .and_then(|v| v.as_str())
                .map(|v| (ctx.clone(), FieldValue::from(v)))
        }))
    }

    fn resolve_neighbors(
        &self,
        contexts: ContextIterator<'vertex, Self::Vertex>,
        _type_name: &str,
        edge_name: &str,
        _parameters: &EdgeParameters,
    ) -> ContextOutcomeIterator<'vertex, Self::Vertex, VertexIterator<'vertex, Self::Vertex>> {
        let edge_name = edge_name.to_string();
        Box::new(contexts.filter_map(move |context| {
            let edge_name = edge_name.clone();
            let active = context.active_vertex().unwrap().clone();

            if edge_name == "*" && active.0.is_sequence() {
                let children: Vec<_> = active
                    .0
                    .as_sequence()
                    .unwrap()
                    .into_iter()
                    .map(|v| Vertex::from(v))
                    .collect();

                return Some((context, Box::new(children.into_iter()) as Box<_>));
            }

            if let Some(value) = active.0.get(edge_name) {
                let children = vec![Vertex::from(value)].into_iter();
                return Some((context.clone(), Box::new(children) as Box<_>));
            }

            None
        }))
    }

    fn resolve_coercion(
        &self,
        _contexts: ContextIterator<'vertex, Self::Vertex>,
        _type_name: &str,
        _coerce_to_type: &str,
    ) -> ContextOutcomeIterator<'vertex, Self::Vertex, bool> {
        todo!()
    }
}

struct Query(KdlDocument);

type Vertices = BTreeMap<Vid, IRVertex>;
type Edges = BTreeMap<Eid, Arc<IREdge>>;
type Outputs = BTreeMap<Arc<str>, TFOutput>;

fn construct_edges(
    doc: &KdlDocument,
    parent_vid: Vid,
    vid_maker: &mut impl Iterator<Item = Vid>,
    eid_maker: &mut impl Iterator<Item = Eid>,
) -> (Vertices, Edges, Outputs) {
    let mut vertices = Vertices::new();
    let mut edges = Edges::new();
    let mut outputs = Outputs::new();

    for node in doc.nodes() {
        let next_vid = vid_maker.next().unwrap();
        let name = node.name().value();
        dbg!(name);

        vertices.insert(
            next_vid,
            IRVertex {
                vid: next_vid,
                type_name: Arc::from("node"),
                coerced_from_type: None,
                filters: Vec::new(),
            },
        );

        if let Some(entry) = node.entries().first() {
            let v = entry.value().to_string();

            let output_name = v
                .strip_prefix("\"@")
                .and_then(|v| v.strip_suffix("\""))
                .unwrap_or(&v);

            if v.starts_with(r#""@"#) {
                outputs.insert(
                    Arc::from(output_name),
                    TFOutput {
                        name: Arc::from(name),
                        value_type: Type::new("String").unwrap(),
                        vid: parent_vid,
                    },
                );
            }
        }

        let parent_to_needle = eid_maker.next().unwrap();

        edges.insert(
            parent_to_needle,
            Arc::new(IREdge {
                eid: parent_to_needle,
                from_vid: parent_vid,
                to_vid: next_vid,
                edge_name: Arc::from(name),
                parameters: EdgeParameters::default(),
                optional: false,
                recursive: None,
            }),
        );

        if let Some(d) = node.children() {
            let (v, e, o) = construct_edges(d, next_vid, vid_maker, eid_maker);
            vertices.extend(v);
            edges.extend(e);
            outputs.extend(o);
        }
    }
    (vertices, edges, outputs)
}

impl Query {
    pub fn iquery_and_arguments(self) -> (IndexedQuery, BTreeMap<Arc<str>, FieldValue>) {
        let mut vid_maker =
            successors(Some(1), |n| Some(n + 1)).map(|n| Vid::new(NonZeroUsize::new(n).unwrap()));
        let mut eid_maker =
            successors(Some(1), |n| Some(n + 1)).map(|n| Eid::new(NonZeroUsize::new(n).unwrap()));
        let _variable_id_maker = successors(Some(1), |n| Some(n + 1)).map(|n| n.to_string());

        let mut vertices = BTreeMap::default();
        let mut edges = BTreeMap::default();

        let starting_vid = vid_maker.next().unwrap();

        vertices.insert(
            starting_vid,
            IRVertex {
                vid: starting_vid,
                type_name: Arc::from("node"),
                coerced_from_type: None,
                filters: Vec::new(),
            },
        );

        // let starting_point = self.0.get("doc").expect("Every query must start with doc");

        let (v, e, o) = construct_edges(&self.0, starting_vid, &mut vid_maker, &mut eid_maker);
        vertices.extend(v);
        edges.extend(e);

        let query_component = IRQueryComponent {
            root: starting_vid,
            vertices,
            edges,
            folds: Default::default(),
            outputs: o
                .iter()
                .map(|(key, output)| {
                    (
                        key.clone(),
                        ContextField {
                            vertex_id: output.vid,                 // on this Vertex...
                            field_name: output.name.clone(),       // ...look for this field...
                            field_type: output.value_type.clone(), // ...with this type...
                        },
                    )
                })
                .collect(),
        };

        let ir_query = IRQuery {
            root_name: Arc::from("Document"),
            root_parameters: EdgeParameters::default(),
            root_component: Arc::from(query_component),
            variables: BTreeMap::new(),
        };

        let mut query: IndexedQuery = ir_query.try_into().unwrap();
        query.outputs = o;
        let arguments = BTreeMap::new();

        (query, arguments)
    }
}

type Outcomes = Vec<BTreeMap<String, FieldValue>>;

pub fn run(raw_query: &str, yaml: &str) -> Result<Outcomes, anyhow::Error> {
    let kdl_doc = kdl::KdlDocument::from_str(raw_query).unwrap();
    let root = serde_yaml::from_str(yaml).unwrap();

    let (query, variables) = Query(kdl_doc).iquery_and_arguments();

    let adapter = YamlAdapter {
        root: Arc::new(root),
    };

    let result: Vec<_> = trustfall_core::interpreter::execution::interpret_ir(
        Arc::new(adapter),
        Arc::new(query),
        Arc::new(variables),
    )?
    .map(|vals| {
        vals.into_iter()
            .map(|(key, field)| (key.to_string(), field))
            .collect::<BTreeMap<_, _>>()
    })
    .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::assert_eq;
    use std::collections::BTreeMap;

    use trustfall::FieldValue;

    use crate::run;

    #[test]
    fn it_works() {
        let pretend_query = indoc::indoc! {r#"
            kind "Deployment"
            metadata {
                name "@name"
            }
            spec {
                template {
                    metadata {
                        annotations {
                            "kube2iam/role" "@role"
                        }
                    }
                    spec {
                        containers {
                            * {
                                image "@image"
                            }
                        }
                    }
                }
            }
        "#};

        let yaml = indoc::indoc! { r#"
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: other-server
              foo: bar
            spec:
              template:
                metadata:
                  annotations:
                    kube2iam/role: "some-fancy-ARN-that-does-not-matter"
                spec:
                  containers:
                  - image: truelayer-docker.jfrog.io/clients-api:v1.44.19
                  - image: truelayer-docker.jfrog.io/nginx-sidecar:v1.1.11
                    name: proxy
                  - image: truelayer-docker.jfrog.io/envoyproxy_envoy:v1.17.0
                    name: proxy-envoy
        "# };

        let hits = run(pretend_query, yaml).unwrap();

        assert_eq!(
            hits,
            vec![
                BTreeMap::from([
                    ("name".into(), FieldValue::from("other-server")),
                    (
                        "role".into(),
                        FieldValue::from("some-fancy-ARN-that-does-not-matter")
                    ),
                    (
                        "image".into(),
                        FieldValue::from("truelayer-docker.jfrog.io/clients-api:v1.44.19")
                    ),
                ]),
                BTreeMap::from([
                    ("name".into(), FieldValue::from("other-server")),
                    (
                        "role".into(),
                        FieldValue::from("some-fancy-ARN-that-does-not-matter")
                    ),
                    (
                        "image".into(),
                        FieldValue::from("truelayer-docker.jfrog.io/nginx-sidecar:v1.1.11")
                    ),
                ]),
                BTreeMap::from([
                    ("name".into(), FieldValue::from("other-server")),
                    (
                        "role".into(),
                        FieldValue::from("some-fancy-ARN-that-does-not-matter")
                    ),
                    (
                        "image".into(),
                        FieldValue::from("truelayer-docker.jfrog.io/envoyproxy_envoy:v1.17.0")
                    ),
                ]),
            ]
        )
    }
}

// doc {
//     kind Deployment
//     metadata {
//         name @name
//     }
//     spec {
//         template {
//             spec {
//                 containers {
//                     * {
//                         image @image
//                     }
//                 }
//             }
//         }
//     }
// }
