#' @title
#' Importar arquivo de folha de pagamento
#'
#' @description
#' Importa o arquivo X4 do sistema DPI para o R, para o DuckDB ou para o Postgres.
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
#'  Se for importar apenas para o R, lembre-se de nomear um objeto para
#'  o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#'  criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#'  No caso de importar para o Postgres será necessário um usuário e uma senha e
#'  o servidor deverá estar escutando o IP do usuário.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_x4("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_x4(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "x4_202202")
#'
#' # para importar para o Postgres:
#' import_x4(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' db = "banestes"
#' tabela = "x4_202202")
#' }
#'


import_x4 = function(
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

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )
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

  # especificacoes arquivo PF
  colunas = c(
      "sip", "empresa", "matricula",
      "tp_pgto", "vlr_pgto", "dt_credito",
      "float", "trf_cheque", "trf_recibo",
      "trf_cc", "trf_cartao_slr", "trf_cms",
      "vlr_trf_cheque", "vlr_trf_recibo",
      "vlr_trf_cc", "vlr_trf_cartao_slr", "vlr_trf_cms",
      "cpfcnpj_empresa", "ag_fav", "ag_empresa",
      "tp_empresa", "cc_empresa", "nr_remessa",
      "nome_fav", "cpf_fav", "cc_fav", "vlr_port"
    )

    larguras = c(
      5, 30, 9, 1, 11, 8, 2, 12, 12, 12, 12, 12,
      11, 11, 11, 11, 11, 14, 4, 4, 1, 11, 5, 100,
      14, 11, 21
    )

    col_types = readr::cols(
        vlr_pgto = readr::col_double(),
        trf_cheque = readr::col_double(),
        trf_recibo = readr::col_double(),
        trf_cc = readr::col_double(),
        trf_cartao_slr = readr::col_double(),
        trf_cms = readr::col_double(),
        vlr_trf_cheque = readr::col_double(),
        vlr_trf_recibo = readr::col_double(),
        vlr_trf_cc = readr::col_double(),
        vlr_trf_cartao_slr = readr::col_double(),
        vlr_trf_cms = readr::col_double(),
        vlr_port = readr::col_double(),
        dt_credito = readr::col_date("%Y%m%d"),
        cpfcnpj_empresa = readr::col_character(),
        ag_fav = readr::col_character(),
        ag_empresa = readr::col_character(),
        cpfcnpj_empresa = readr::col_character(),
        cpf_fav = readr::col_character(),
        cc_fav = readr::col_character()
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
      col_types = col_types
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")

    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # adicionando dicionário
  data = within(data, {
      dsc_tp_pgto = NA
      dsc_tp_empresa = NA
      dsc_tp_pgto[tp_pgto == 3] = "conta corrente"
      dsc_tp_pgto[tp_pgto == 6] = "conta meu salario"
      dsc_tp_pgto[tp_pgto == 7] = "conta salario eletronico"
      dsc_tp_empresa[tp_empresa %in% c("A", "B", "C", "D", "L")] = "estadual"
      dsc_tp_empresa[tp_empresa %in% c("E", "M")] = "municipal"
      dsc_tp_empresa[tp_empresa %in% c("F", "N")] = "privado"
      dsc_tp_empresa[tp_empresa %in% c("G")] = "federal"
      dsc_tp_empresa[tp_empresa %in% c("I")] = "pf"
      dsc_tp_empresa[tp_empresa %in% c("J")] = "consignacao"
  })

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas")
  ini = as.numeric(format(Sys.time(), "%S"))

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  fim = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_success("Linhas removidas em {(fim - ini)} segundos")

  # criar tabela no DuckDB
  if (import_db != "none") {

  # dropar tabela
  cli::cli_alert_info("Dropando tabela")
  DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
  cli::cli_alert_success("Tabela dropada")

  # gravar tabela
  ini = as.numeric(format(Sys.time(), "%M"))
  cli::cli_alert_info("Iniciando gravação no DuckDB")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data)

  fim = as.numeric(format(Sys.time(), "%M"))
  cli::cli_alert_success("Tabela gravada em {(fim - ini)}")

  # gravar índices
  cli::cli_alert_info("Iniciando gravação de índices")

  if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX idx_x4_cpfcnpj (cpfcnpj_empresa(14))"
      ))
  }

  if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_x4_cpfcnpj ON ", tabela, " (cpfcnpj_empresa);"))
  }

  cli::cli_alert_success("Índices gravados")

  # desconectando
 DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
