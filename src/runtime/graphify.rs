use crate::{
    contracts::CoreResult,
    graph::{graphify_record, merge_graph, GraphHint, GraphifyOutput},
    memory::MemoryRecord,
    store::GraphStore,
};

pub fn graphify_memory(
    store: &mut impl GraphStore,
    record: &MemoryRecord,
    hints: &[GraphHint],
) -> CoreResult<GraphifyOutput> {
    let output = graphify_record(record, hints)?;
    merge_graph(store, output.clone())?;
    Ok(output)
}
