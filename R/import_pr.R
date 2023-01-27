#' @title
#' Importar arquivo da base geral produtos GBPAG0
#'
#' @description
#' Importa o arquivo GBPAGO para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#'  String. O caminho do arquivo a ser importado, incluindo a
#'  extensao (.txt).
#' @param import_db
#'  String. "none", "duckdb" ou "postgres".
#' @param usuario
#'  String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#'  String. Se import_db = "postgres", informar o IP do servidor.
#' @param port
#'  Integer. Se import_db = "postgres", informar a porta do servidor.
#' @param senha
#'  String. Se import_db = "postgres", a senha deverá ser informada.
#' @param db
#'  String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
#'  Se import_db = "postgres", o nome do DB.
#' @param metodo
#'  String. "readr" ou "vroom".
#' @param tabela
#'  String. Nome da tabela a ser criada no DuckDB.
#'
#' @details
#' Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' No caso de importar para o Postgres será necessário um usuário e uma senha e
#' o servidor deverá estar escutando o IP do usuário.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data <- import_pr("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_pr(
#'   "\\path\\to\\file.txt",
#'   import_db = "duckdb",
#'   db = "\\path\\to\\duck.db",
#'   tabela = "pr"
#' )
#'
#' # para importar para o Postgres:
#' import_pr(
#'   "\\path\\to\\file.txt",
#'   import_db = "postgres",
#'   usuario = "user",
#'   senha = "password",
#'   bd = "banestes",
#'   tabela = "pr_202202"
#' )
#' }
#'
import_pr <- function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela,
  metodo = "vroom") {

  # evaluate arg tabela
  if (import_db != "none" && is.null(tabela)) {
    stop("tabela deve ser informada")
  }

  # evaluate arg usuario
  if (import_db == "postgres" && is.null(usuario)) {
    stop("usuário deve ser informado")
  }

  # evaluate arg senha
  if (import_db == "postgres" && is.null(senha)) {
    stop("senha deve ser informada")
  }

  # evaluate arg db
  if (import_db != "none" && is.null(db)) {
    stop("database deve ser informado.
    Se DuckDB, o caminho. Se Postgres, o nome do DB")
  }

  # evaluate arg import_db
  match.arg(
    arg = import_db,
    choices = c("none", "duckdb", "postgres"),
    several.ok = FALSE
  )

  # evaluate arg metodo
  match.arg(
    arg = metodo,
    choices = c("vroom", "readr"),
    several.ok = FALSE
  )

  # conexao com DuckDB

  if (import_db == "duckdb") {
    cli::cli_alert_info("Iniciando conexão com DuckDB")

    con <- DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com DuckDB")
  }

  # conexao com postgres

  if (import_db == "postgres") {
    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con <- DBI::dbConnect(
      RPostgres::Postgres(),
      host = host,
      port = port,
      user = usuario,
      password = senha,
      dbname = db
    )

    cli::cli_alert_success("Conectado com o banco de dados")
  }

  # especificacoes arquivo
  colunas <- c(
    "cod_produto", "cod_subproduto", "descricao_prod", "cod_categoria",
    "descricao_finalid", "nome_reduzido_prod", "instru_norm_prod",
    "id_situacao_prod", "sistem_origem_prod", "unid_prazo_operac",
    "id_tipo_cobranca", "id_debito_conta", "id_tipo_remun",
    "obrigat_c_corrente", "id_renov_auto_glob", "codigo_carteira",
    "dt_criacao_prod", "dt_inicio_comerc_prod", "dt_fim_comerc_prod",
    "cod_orgao_gestor_prod", "tipo_cliente", "prazo_min_operac",
    "prazo_max_operac", "sald_min_manut_aplic", "valor_min_aplic_adic",
    "perc_recolhi_comp", "prazo_min_caren_pgto_prod",
    "prazo_max_caren_pagto_prod",
    "valor_lim_min", "valor_lim_max", "perc_tx_adm", "valor_min_resg",
    "qtd_max_parc", "qtd_min_parc", "qtd_dias_atraso", "dt_inclusao_prod",
    "qtd_min_avalistas", "id_autor_form_contrato", "id_autor,efetiv_contrato",
    "id_renov_indiv", "id_aceita_prorrog_contrato", "id_renegociacao",
     "tp_tx_aceita_prod", "prazo_inform_tx", "form_aplic_tx_juros",
    "ident_tp_avalista", "ident_risco", "ident_unicid_cliente",
    "percent_multa_atraso_pgto", "lim_cred_pre_determ",
    "ident_integ_gestao", "dt_integ_gestao", "perc_lim_cred",
    "ident_avaliacao", "ident_repac", "ident_transf_titul",
    "ident_comercializacao", "ident_tp_cred", "cod_modal",
    "permite_dupli_reneg", "ident_cob_amig", "dias_env_cob_amig",
    "ident_empresa", "cod_natureza_op", "ident_alcada_inf_comp",
    "dias_env_empresa_cob", "ident_saldo_med_mensal",
    "ident_saldo_aplic_financ", "ident_inform_op",
    "prazo_prorrog_contrato_venc", "prazo_renov_contrato_venc",
    "prazo_reav_contrato_venc", "prazo_repac_contrato_venc",
    "prazo_transf_tit_contrato_venc", "ident_transf_avalista",
    "prazo_transf_aval_contrato_venc", "ident_transf_garantia",
    "prazo_transf_garant_contr_venc", "ident_partic_calc_vlr_risco",
    "ident_exig_cad_corrent", "ident_gp_dist_rating",
    "ident_control_desc_reneg", "ident_control_perm_reneg",
    "ident_gp_dist_score", "ident_aprov_auto_alcada_min",
    "ident_prioriz_deb", "identif_interno_conta",
    "saldo_dev_max_client", "saldo_med_max_cliente",
    "cod_tbl_adv_gbr", "id_analise_tbl_adv",
    "tp_acao_tbl_adv", "prod_renda_apv", "perm_form_proponente"
  )

  larguras <- c(
    4, 4, 60, 1, 300, 15, 60, 1, 3, 1, 1, 1, 1, 1, 1,
    3, 8, 8, 8, 4, 1, 4, 4, 13, 13, 5, 4, 4, 13, 13,
    4, 13, 3, 3, 4, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 4, 1, 1, 8, 4, 1, 1, 1, 1, 1, 4, 1, 1, 3, 2, 3, 1,
    3, 1, 1, 1, 3, 3, 3, 3, 3, 1, 3, 1, 3, 1, 1, 2,
    1, 1, 2, 1, 1, 5, 16, 16, 2, 1, 1, 1, 1
  )

  col_types <- readr::cols(
    dt_criacao_prod = readr::col_date("%Y%m%d"),
    dt_inicio_comerc_prod = readr::col_date("%Y%m%d"),
    dt_fim_comerc_prod = readr::col_date("%Y%m%d"),
    dt_inclusao_prod = readr::col_date("%Y%m%d"),
    dt_integ_gestao = readr::col_date("%Y%m%d"),
    percent_mult_atrado_pgto = readr::col_double(),
    tipo_cliente = readr::col_character(),
    perc_recolhi_comp = readr::col_double(),
    perc_tx_adm = readr::col_double(),
    sald_min_manut_aplic = readr::col_double(),
    valor_min_aplic_adic = readr::col_double(),
    valor_lim_min = readr::col_double(),
    valor_lim_max = readr::col_double(),
    valor_min_resg = readr::col_double(),
    saldo_dev_max_client = readr::col_double(),
    saldo_med_max_cliente = readr::col_double(),
  )

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    cli::cli_alert_info("Iniciando importação com readr")

    data <- readr::read_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    cli::cli_alert_info("Iniciando importação com vroom")
    ini <- Sys.time()

    data <- vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

    fim <- Sys.time()
    cli::cli_alert_success("Arquivo importado em {(fim - ini) / 60} min")
  }

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas")
  ini <- as.numeric(format(Sys.time(), "%S"))

  data <- data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  fim <- as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_success("Linhas removidas em {(fim - ini)} segundos")

  # criar tabela no DuckDB
  if (import_db != "none") {

    # dropar tabela
    cli::cli_alert_info("Dropando tabela")
    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
    cli::cli_alert_success("Tabela dropada")

    # gravar tabela
    ini <- Sys.time()
    cli::cli_alert_info("Iniciando gravação no DuckDB")

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    fim <- Sys.time()
    cli::cli_alert_success("Tabela gravada em {(fim - ini)}")

    # gravar índices
    cli::cli_alert_info("Iniciando gravação de índices")

    if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX idx_pr_cod_produto (cod_produto(4)),
      ADD INDEX idx_pr_cod_subproduto (cod_subproduto(4)),
      ADD INDEX idx_pr_dt_criacao_prod (dt_criacao_prod(8)),
      ADD INDEX dt_pr_dt_inicio_comerc_prod (dt_inicio_comerc_prod(8)),
      ADD INDEX idx_pr_dt_fim_comerc_prod (dt_fim_comerc_prod(8));
      "))
    }

    if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_pr_cod_produto ON ", tabela, " (cod_produto);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_pr_cod_subproduto ON ", tabela, " (cod_subproduto);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_pr_dt_criacao_prod ON ", tabela, " (dt_criacao_prod);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX dt_pr_dt_inicio_comerc_prod ON ",
      tabela, " (dt_inicio_comerc_prod);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_pr_dt_fim_comerc_prod ON ",
      tabela, " (dt_fim_comerc_prod);"))
    }

    cli::cli_alert_success("Indices criados")

    # desconectando
    DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())
  } else {
    data
  }
}
