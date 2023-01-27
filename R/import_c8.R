#' @title
#' Importar arquivo da base geral de clientes do GCRED – CFFAC8
#'
#' @description
#' Importa o arquivo c8 para o R, para o DuckDB ou para o Postgres.
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
#' data = import_c8("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_c8(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#' tabela = c8_202202)
#'
#' # para importar para o Postgres:
#' import_c8(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "c8_202202")
#' }
#'


import_c8 = function(
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

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com o banco de dados")
  }

  # conexao com postgres

  if (import_db == "postgres") {

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con = DBI::dbConnect(
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
    colunas = c(
    "n_contrato", "nome", "conta", "agencia", "situacao",
    "n_conta_contabil", "dt_emissao", "dt_vencimento", "tx_juros",
    "indice_correcao", "qt_prestacoes", "valor_principal_aberto",
    "saldo_devedor", "rendas_a_apropriar", "saldo_contabil",
    "dt_venc_ult_prestacao_pg", "dt_pagamento_ult_prestacao_pg",
    "dt_venc_prim_prestacao_aberto", "garantias_prestadas",
    "prazo_total_dias", "total_dias_atraso", "situacao_atraso",
    "produto", "subproduto", "saldo_devedor_vencido", "cpfcnpj",
    "dt_transf_contabil", "dt_inclusao_contrato", "valor_cred_aberto",
    "class_contrato", "valor_aplicacao", "empresa", "cod_beneficio",
    "tipo_cliente", "dt_nascimento", "sexo", "estado_civil", "profissao",
    "debito_conta", "valor_rendas_juros_mes", "cid",
    "valor_prestacao_original", "motivo_descaracterizacao",
    "receita_efetivada_correcao_monetaria",
    "renda_apropriar_renegociada", "rendas_efetivadas_mes",
    "n_versao_contrato_cff", "ano_origem_contrato",
    "n_pn_origem_contrato",
    "qtd_parcelas_pg", "canal_orig_contrato"
    )

    larguras = c(
      12, 50, 11, 4, 1, 5, 8, 8, 8, 4, 2, 12, 12, 12, 12, 8, 8, 8, 20, 4,
      4, 1, 4, 4, 12, 14, 8, 8, 14, 2, 16, 6, 20, 1, 8, 1, 10, 60, 1, 12,
      4, 12, 1, 15, 12, 12, 2, 4, 7, 3, 25
      )

    col_types = readr::cols(
      n_contrato = readr::col_character(),
      tx_juros = readr::col_double(),
      dt_emissao = readr::col_date("%Y%m%d"),
      dt_vencimento = readr::col_date("%Y%m%d"),
      dt_venc_ult_prestacao_pg = readr::col_date("%Y%m%d"),
      dt_pagamento_ult_prestacao_pg = readr::col_date("%Y%m%d"),
      dt_venc_prim_prestacao_aberto = readr::col_date("%Y%m%d"),
      dt_transf_contabil = readr::col_date("%Y%m%d"),
      dt_inclusao_contrato = readr::col_date("%Y%m%d"),
      dt_nascimento = readr::col_date("%Y%m%d"),
      qtd_parcelas_pg = readr::col_double(),
      valor_principal_aberto = readr::col_double(),
      saldo_devedor = readr::col_double(),
      saldo_devedor_vencido = readr::col_double(),
      canal_orig_contrato = readr::col_double(),
      rendas_a_apropriar = readr::col_double(),
      saldo_contabil = readr::col_double(),
      valor_cred_aberto = readr::col_double(),
      valor_prestacao_original = readr::col_double()
    )

# importar arquivo para o R via {readr}
  if (metodo == "readr") {

    cli::cli_alert_info("Iniciando importação com readr")

    data = readr::read_fwf(
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

    ini = Sys.time()
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

    fim = Sys.time()
    cli::cli_alert_success("Arquivo importado em {(fim - ini)}")
  }

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas")
  ini = Sys.time()

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  fim = Sys.time()
  cli::cli_alert_success("Linhas removidas em {(fim - ini)}")


  # criar tabela no DuckDB
  if (import_db != "none") {

  # dropar tabala
  cli::cli_alert_info("Dropando tabela")
  DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
  cli::cli_alert_success("Tabela dropada")

  # gravar tabela
  ini = Sys.time()
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data)

  fim = Sys.time()
  cli::cli_alert_success("Tabela gravada em {(fim - ini)}")

  # gravar índices
  cli::cli_alert_info("Iniciando gravação de índices")

  if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX idx_c8_cpfcnpj (cpfcnpj(14)),
      ADD INDEX idx_c8_agencia (agencia(4)),
      ADD INDEX idx_c8_dt_emissao (dt_emissao(8)),
      ADD INDEX idx_c8_dt_vencimento (dt_vencimento(8));
      "))
    }

  if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_c8_cpfcnpj ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_c8_agencia ON ", tabela, " (agencia);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_c8_dt_emissao ON ", tabela, " (dt_emissao);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_c8_dt_vencimento ON ", tabela, " (dt_vencimento);"))
  }

  cli::cli_alert_success("Indices criados")

  # desconectando
  duckdb::dbDisconnect(con, shutdown = TRUE)
  remove(con)
  invisible(gc())
  cli::cli_alert_success("Conexão encerrada")

  } else {

    data

    }
}
