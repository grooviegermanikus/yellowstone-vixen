use yellowstone_vixen_proc_macro::include_vixen_parser;

include_vixen_parser!("/Users/stefan/mango/code/jupiter-yolo-trader/crates/yolotrader_programs/idls/meteora_dbc.codama.json");

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use serde_json::json;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_request::RpcRequest;
    use solana_client::rpc_response::transaction::Signature;
    use yellowstone_vixen_core::{Parser, ProgramParser, Pubkey};
    use yellowstone_vixen_core::instruction::InstructionUpdate;
    use crate::{get_rpc_client, load_fixture, try_from_tx_meta, tx_fixture, SerializableInstructionUpdate, RPC_ENDPOINT};
    use crate::debug_meteora_dbc::dynamic_bonding_curve::InstructionParser;

    struct DummyProgramParser {
        program_id: String,
    }

    #[tokio::test]
    async fn it_works() {

        // let sig = "zRJQjFJEzrABHuk3F4N4ac18WevLGtT6aQc1UbkgyvY8hqVC5HacGtEnTWx2GGMBK3Tr3aJ9xSQGKiTLQz8PfuZ";
        let sig = "3ytxyuDYCy2G1ghyba9ihRvry1s2LE5afwD56GBGkeTkps1omB5YYdnMcZtjT6y4Q1Qx7wGiHJqAwwCueJ3HxMou";

        let signature = Signature::from_str(sig).unwrap();
        const RPC_ENDPOINT: &str = "https://api.mainnet.solana.com";
        let rpc_client = RpcClient::new(RPC_ENDPOINT.to_string());

        let params = json!([signature.to_string(), {
                "encoding": "json",
                "maxSupportedTransactionVersion": 0
            }]);

        let tx = rpc_client
            .send(RpcRequest::GetTransaction, params)
            .await
            .map_err(|e| format!("Error fetching tx: {e:?}")).unwrap();

        let instructions: Vec<SerializableInstructionUpdate> = try_from_tx_meta(tx, "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN".to_string()).unwrap();
        // println!("Instructions: {:#?}", instructions);

        let parser = InstructionParser;
        for instr in instructions {
            let ix_update: InstructionUpdate = InstructionUpdate::from(&instr);

            const EVENT_IX_TAG: u64 = 0x1d9acb512ea545e4;
            const EVENT_IX_TAG_LE: [u8; 8] = EVENT_IX_TAG.to_le_bytes();

            if ix_update.program != Pubkey::from_str("dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN").unwrap() {
                continue;
            }

            if let Some(slice) = ix_update.data.get(0usize..8usize) {
                if slice == EVENT_IX_TAG_LE {
                    continue; // this a CPI instruction, skip it
                }
            }

            let data_str = hex::encode(&ix_update.data);
            println!("Parsing instruction: {:?}: {}", ix_update.ix_path.as_ref().unwrap(), data_str);
            let parsed = parser.parse(&ix_update).await.unwrap();
            println!("> {:?}", parsed);
        }


    }
}