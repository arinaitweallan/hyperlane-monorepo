use std::future::Future;
use std::time::Instant;

use cosmrs::proto::tendermint::blocksync::BlockResponse;
use hyperlane_core::rpc_clients::BlockNumberGetter;
use hyperlane_metric::prometheus_metric::{
    JsonRpcClientMetrics, PrometheusJsonRpcClientConfig, PrometheusJsonRpcClientConfigExt,
};
use maplit::hashmap;
use tendermint::Hash;
use tendermint_rpc::client::CompatMode;
use tendermint_rpc::endpoint::{block, block_by_hash, block_results, tx};
use tendermint_rpc::{Client, HttpClient, HttpClientUrl, Url as TendermintUrl};

use hyperlane_core::{ChainCommunicationError, ChainResult};
use tonic::async_trait;
use url::Url;

use crate::{ConnectionConf, HyperlaneCosmosError};

/// Thin wrapper around Cosmos RPC client with error mapping
#[derive(Clone, Debug)]
pub struct CosmosRpcClient {
    client: HttpClient,
    metrics: JsonRpcClientMetrics,
    metrics_config: PrometheusJsonRpcClientConfig,
}

impl CosmosRpcClient {
    /// Create new `CosmosRpcClient`
    pub fn new(
        url: &Url,
        metrics: JsonRpcClientMetrics,
        metrics_config: PrometheusJsonRpcClientConfig,
    ) -> ChainResult<Self> {
        let tendermint_url = tendermint_rpc::Url::try_from(url.to_owned())
            .map_err(Into::<HyperlaneCosmosError>::into)?;
        let url = tendermint_rpc::HttpClientUrl::try_from(tendermint_url)
            .map_err(Into::<HyperlaneCosmosError>::into)?;

        let client = HttpClient::builder(url)
            // Consider supporting different compatibility modes.
            .compat_mode(CompatMode::latest())
            .build()
            .map_err(Into::<HyperlaneCosmosError>::into)?;

        Ok(Self {
            client,
            metrics,
            metrics_config,
        })
    }

    /// Request block by block height
    pub async fn get_block(&self, height: u32) -> ChainResult<block::Response> {
        self.track_metric_call("get_block", || async {
            Ok(self
                .client
                .block(height)
                .await
                .map_err(Into::<HyperlaneCosmosError>::into)?)
        })
        .await
    }

    /// Request block results by block height
    pub async fn get_block_results(&self, height: u32) -> ChainResult<block_results::Response> {
        self.track_metric_call("get_block_results", || async {
            Ok(self
                .client
                .block_results(height)
                .await
                .map_err(Into::<HyperlaneCosmosError>::into)?)
        })
        .await
    }

    /// Request block by block hash
    pub async fn get_block_by_hash(&self, hash: Hash) -> ChainResult<block_by_hash::Response> {
        self.track_metric_call("get_block_by_hash", || async {
            Ok(self
                .client
                .block_by_hash(hash)
                .await
                .map_err(Into::<HyperlaneCosmosError>::into)?)
        })
        .await
    }

    /// Request the latest block
    pub async fn get_latest_block(&self) -> ChainResult<block::Response> {
        self.track_metric_call("get_latest_block", || async {
            Ok(self
                .client
                .latest_block()
                .await
                .map_err(Into::<HyperlaneCosmosError>::into)?)
        })
        .await
    }

    /// Request transaction by transaction hash
    pub async fn get_tx_by_hash(&self, hash: Hash) -> ChainResult<tx::Response> {
        self.track_metric_call("get_tx_by_hash", || async {
            Ok(self
                .client
                .tx(hash, false)
                .await
                .map_err(Into::<HyperlaneCosmosError>::into)?)
        })
        .await
    }

    async fn track_metric_call<F, Fut, T>(&self, rpc_method: &str, rpc_call: F) -> ChainResult<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ChainResult<T>>,
    {
        let start = Instant::now();
        let res = rpc_call().await;

        let labels = hashmap! {
            "provider_node" => self.metrics_config.node_host(),
            "chain" => self.metrics_config.chain_name(),
            "method" => rpc_method,
            "status" => if res.is_ok() { "success" } else { "failure" }
        };
        if let Some(counter) = &self.metrics.request_count {
            counter.with(&labels).inc()
        }
        if let Some(counter) = &self.metrics.request_duration_seconds {
            counter
                .with(&labels)
                .inc_by((Instant::now() - start).as_secs_f64())
        };
        res
    }
}

#[async_trait]
impl BlockNumberGetter for CosmosRpcClient {
    async fn get_block_number(&self) -> Result<u64, ChainCommunicationError> {
        self.get_latest_block()
            .await
            .map(|block| block.block.header.height.value())
    }
}
