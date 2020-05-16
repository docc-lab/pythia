use crate::rpclib::read_client_stats;
use crate::settings::Settings;

pub struct BudgetManager {
    clients: Vec<String>,
}

impl BudgetManager {
    pub fn from_settings(settings: &Settings) -> Self {
        BudgetManager {
            clients: settings.pythia_clients.clone(),
        }
    }

    pub fn print_stats(&self) {
        for client in &self.clients {
            eprintln!("{}: {:?}", client, read_client_stats(client));
        }
    }
}
