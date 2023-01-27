#' @title
#' Importar arquivo de propostas de negocio A8
#'
#' @description
#' Importa o arquivo A8 para o R, para o DuckDB ou para o postgres.
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
#' No caso de importar para o postgres será necessário um usuário e uma senha e
#' o servidor deverá estar escutando o IP do usuário.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_a8("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_a8(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#' tabela = a8)
#'
#' # para importar para o postgres:
#' import_a8(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "a8")
#' }
#'


import_a8 = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela = "a8",
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
    Se DuckDB, o caminho. Se postgres, o nome do DB")
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
    "cid_responsavel", "cid_elaboracao", "cpfcnpj", "cc",
    "ano_pn", "nr_pn", "dt_inclusao_pn", "vlr_proposta",
    "situacao_pn", "alcada", "dt_formalizacao", "dt_aprovacao",
    "dt_efetivacao", "produto", "subproduto",
    "matr_proponente", "matr_formalizacao",
    "matr_efetivacao", "tx_proposta", "id_tx", "prazo", "id_prazo",
    "contrato_origem", "qtd_parcela", "dt_venc_cnt_prorrogado",
    "dt_renegociacao", "tx_cac", "class_pn", "tx_pos", "id_tx_pos",
    "id_ressalva_pn", "id_voto_negativo", "origem_pn",
    "id_analise_liberacao", "tx_negociada",
    sapply(1:30, function(x) paste0("exc", x)),
    unlist(lapply(1:4, function(x) {
                c(
                    paste0("cpfcnpj_avalista_", x),
                    paste0("id_avalista_", x)
                )
            })),
    unlist(lapply(1:3, function(x) {
                c(
                    paste0("cod_bem_", x),
                    paste0("tp_garantia_", x),
                    paste0("percentual_garantia_", x)
                )
            }))
    )

    larguras = c(
      4, 4, 14, 10, 4, 7, 8, 17, 1, 2, 8, 8, 8, 4, 4, 9, 9, 9, 9,
      1, 4, 1, 20, 4, 8, 8, 5, 2, 5,
      4, 1, 1, 1, 1, 7,
      rep(4, 30),
      rep(c(14, 1), 4),
      rep(c(2, 3, 3), 3)
    )

    col_types = readr::cols(
      cid_responsavel = readr::col_character(),
      cid_elaboracao = readr::col_character(),
      cpfcnpj = readr::col_character(),
      cc = readr::col_character(),
      nr_pn = readr::col_character(),
      dt_inclusao_pn = readr::col_date("%Y%m%d"),
      vlr_proposta = readr::col_double(),
      alcada = readr::col_character(),
      dt_formalizacao = readr::col_date("%Y%m%d"),
      dt_aprovacao = readr::col_date("%Y%m%d"),
      dt_efetivacao = readr::col_date("%Y%m%d"),
      dt_renegociacao = readr::col_date("%Y%m%d"),
      tx_cac = readr::col_double(),
      tx_pos = readr::col_double(),
      tx_negociada = readr::col_double(),
      id_analise_liberacao = readr::col_character(),
      cpfcnpj_avalista_1 = readr::col_character(),
      cpfcnpj_avalista_2 = readr::col_character(),
      cpfcnpj_avalista_3 = readr::col_character(),
      cpfcnpj_avalista_4 = readr::col_character(),
      id_avalista_1 = readr::col_character(),
      id_avalista_2 = readr::col_character(),
      id_avalista_3 = readr::col_character(),
      id_avalista_4 = readr::col_character(),
      cod_bem_1 = readr::col_character(),
      tp_garantia_1 = readr::col_character(),
      percentual_garantia_1 = readr::col_double()
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
      skip = 0
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")

    Sys.time()
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 0
    )

    Sys.time()
    cli::cli_alert_success("Arquivo importado")
  }

  # limpar embedded nuls
  cli::cli_alert_info("Limpando embedded nuls")
  data = data[!is.na(data$cid_responsavel),]

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas")
  Sys.time()

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  Sys.time()
  cli::cli_alert_success("Linhas removidas")

  # gravar no banco de dados
  if (import_db != "none") {

  # gravar tabela
  Sys.time()
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)

  Sys.time()
  cli::cli_alert_success("Tabela gravada")

  # desconectando
  duckdb::dbDisconnect(con, shutdown = TRUE)
  remove(con)
  invisible(gc())
  cli::cli_alert_success("Conexão encerrada")

  } else {

    data

    }
}
