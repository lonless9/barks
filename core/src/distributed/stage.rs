//! DAG (Directed Acyclic Graph) Scheduler for breaking an RDD graph into stages.
//!
use crate::traits::{Dependency, RddBase, ShuffleDependencyInfo};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::info;

/// A stage of computation, containing a set of tasks that can be run in parallel.
/// Stages are separated by shuffle boundaries.
#[derive(Clone, Debug)]
pub struct Stage {
    pub id: usize,
    /// The RDD that this stage computes. Stored as `Any` to handle different RDD types.
    pub rdd: Arc<dyn Any + Send + Sync>,
    pub num_partitions: usize,
    /// The stages that this stage depends on.
    pub parents: Vec<Arc<Stage>>,
}

/// A final stage in a job that computes a result or action.
#[derive(Clone, Debug)]
pub struct ResultStage {
    pub stage: Stage,
}

/// A stage that writes shuffle data as output.
#[derive(Clone, Debug)]
pub struct ShuffleMapStage {
    pub stage: Stage,
    pub dependency: ShuffleDependencyInfo,
}

/// DAG Scheduler for managing RDD lineage and stage creation.
#[derive(Clone)]
pub struct DAGScheduler {
    next_stage_id: Arc<AtomicUsize>,
}

impl DAGScheduler {
    pub fn new() -> Self {
        Self {
            next_stage_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Creates the final stage for a job, which is a `ResultStage`.
    /// This function serves as the entry point for building the entire stage graph for a job.
    pub fn new_result_stage<T>(
        &self,
        rdd: Arc<dyn Any + Send + Sync>,
        rdd_base: Arc<dyn RddBase<Item = T>>,
        job_id: &str,
    ) -> ResultStage
    where
        T: crate::traits::Data,
    {
        info!(
            "Creating new result stage for RDD {} in job {}",
            rdd_base.id(),
            job_id
        );
        let mut rdd_to_stage: HashMap<usize, Arc<Stage>> = HashMap::new();
        let parents = self.get_or_create_parent_stages(rdd_base.clone(), &mut rdd_to_stage);

        let stage = Stage {
            id: self.new_stage_id(),
            rdd,
            num_partitions: rdd_base.num_partitions(),
            parents,
        };
        ResultStage { stage }
    }

    /// Recursively creates parent stages for a given RDD.
    /// It traverses the RDD dependency graph backwards from the given RDD.
    /// A new stage is created at each shuffle boundary.
    fn get_or_create_parent_stages<T>(
        &self,
        rdd_base: Arc<dyn RddBase<Item = T>>,
        rdd_to_stage: &mut HashMap<usize, Arc<Stage>>,
    ) -> Vec<Arc<Stage>>
    where
        T: crate::traits::Data,
    {
        let mut parents = Vec::new();
        let mut waiting = std::collections::VecDeque::new();
        waiting.push_back(rdd_base);

        while let Some(rdd) = waiting.pop_front() {
            if let Some(stage) = rdd_to_stage.get(&rdd.id()) {
                parents.push(stage.clone());
            } else {
                let mut shuffle_parents = Vec::new();
                for dep in rdd.dependencies() {
                    if let Dependency::Shuffle(parent_rdd_any, shuffle_info) = dep {
                        // Downcast parent RDD to call `RddBase` methods
                        let parent_rdd_base = parent_rdd_any
                            .downcast_ref::<Arc<dyn RddBase<Item = T>>>()
                            .expect("Parent RDD is not a valid RddBase object")
                            .clone();

                        // Create a ShuffleMapStage for this parent.
                        let shuffle_map_stage = self.get_or_create_shuffle_map_stage(
                            parent_rdd_base,
                            shuffle_info,
                            rdd_to_stage,
                        );
                        shuffle_parents.push(shuffle_map_stage);
                    } else if let Dependency::Narrow(parent_rdd_any) = dep {
                        // Narrow dependency, so continue traversing up the same stage.
                        let parent_rdd_base = parent_rdd_any
                            .downcast_ref::<Arc<dyn RddBase<Item = T>>>()
                            .expect("Parent RDD is not a valid RddBase object")
                            .clone();
                        waiting.push_back(parent_rdd_base);
                    }
                }
                if !shuffle_parents.is_empty() {
                    parents.extend(shuffle_parents);
                }
            }
        }
        parents
    }

    /// Creates a `ShuffleMapStage` for a given shuffle dependency, or returns an existing one.
    fn get_or_create_shuffle_map_stage<T>(
        &self,
        parent_rdd_base: Arc<dyn RddBase<Item = T>>,
        _shuffle_dep: ShuffleDependencyInfo,
        rdd_to_stage: &mut HashMap<usize, Arc<Stage>>,
    ) -> Arc<Stage>
    where
        T: crate::traits::Data,
    {
        if let Some(stage) = rdd_to_stage.get(&parent_rdd_base.id()) {
            return stage.clone();
        }

        let parent_stages = self.get_or_create_parent_stages(parent_rdd_base.clone(), rdd_to_stage);
        let stage = Arc::new(Stage {
            id: self.new_stage_id(),
            rdd: unsafe { std::mem::transmute(parent_rdd_base.clone()) },
            num_partitions: parent_rdd_base.num_partitions(),
            parents: parent_stages,
        });

        rdd_to_stage.insert(parent_rdd_base.id(), stage.clone());
        info!(
            "Created new ShuffleMapStage {} for RDD {}",
            stage.id,
            parent_rdd_base.id()
        );
        stage
    }

    fn new_stage_id(&self) -> usize {
        self.next_stage_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Check if all stages are completed (placeholder implementation)
    pub async fn all_stages_completed(&self) -> bool {
        // Placeholder: In a real implementation, this would track stage completion
        true
    }

    /// Get the next ready stage for execution (placeholder implementation)
    pub async fn get_next_ready_stage(&self) -> Option<Arc<Stage>> {
        // Placeholder: In a real implementation, this would return the next stage ready for execution
        None
    }

    /// Submit a job and return the stages (placeholder implementation)
    pub async fn submit_job<T>(
        &self,
        _rdd: Arc<dyn crate::traits::RddBase<Item = T>>,
    ) -> Result<Vec<Arc<Stage>>, crate::traits::RddError>
    where
        T: crate::traits::Data,
    {
        // Placeholder: In a real implementation, this would create and return all stages for the job
        Ok(Vec::new())
    }
}

impl Default for DAGScheduler {
    fn default() -> Self {
        Self::new()
    }
}
