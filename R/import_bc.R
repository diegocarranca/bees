#' @title
#' Importar arquivo da base geral de clientes do GBRABC
#'
#' @description
#' Importa o arquivo GBRABC para o R, para o DuckDB ou para o Postgres.
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
#' data = import_bc("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_bc(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "bc_202201")
#'
#' # para importar para o Postgres:
#' import_bc(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' db = "banestes",
#' tabela = "bc_202201")
#' }
#'


import_bc = function(
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

  # conexao com Postgres
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
   "tp_registro", "tp_cliente", "cpfcnpj", "dt_inicio_advertencia",
   "cod_advertencia", "cod_cliente", "nome", "cpfcnpj_2",
   "n_doc_origem_advertencia", "cod_produto", "cod_sub_produto",
   "dt_fim_advertencia", "id_grau_responsabilidade",
   "dt_controle_processamento", "cod_interno_dependencia",
   "cod_empresa_externa_informante", "qtd_ocorrencia_advertencia",
   "dt_primeira_ocorrencia_advertencia", "dt_ult_ocorrencia_advertencia"
     )


  larguras = c(
  1, 1, 14, 8, 4, 9, 100, 14, 22, 4, 4, 8, 1, 8, 4, 10, 4, 8, 8
  )

  col_types = readr::cols(
    dt_inicio_advertencia = readr::col_date("%Y%m%d"),
    dt_fim_advertencia = readr::col_date("%Y%m%d"),
    dt_primeira_ocorrencia_advertencia = readr::col_date("%Y%m%d"),
    dt_ult_ocorrencia_advertencia = readr::col_date("%Y%m%d"),
    dt_controle_processamento = readr::col_date("%Y%m%d"),
    cpfcnpj = readr::col_character(),
    cpfcnpj_2 = readr::col_character()
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

    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

  cli::cli_alert_success("Arquivo importado {Sys.time()}")
  }

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

    # dropando tabela
    cli::cli_alert_info("Dropando tabela")
    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
    cli::cli_alert_success("Tabela dropada")

    # gravar tabela
    ini = Sys.time()
    cli::cli_alert_info("Iniciando gravação no DuckDB")

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    fim = Sys.time()
    cli::cli_alert_success("Tabela gravada em {(fim - ini)}")

    # gravar indices

    cli::cli_alert_info("Iniciando gravação de índices")

    if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX (cpfcnpj(14)),
      ADD INDEX (cpfcnpj_2(14));
      "))
    }

    if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (cpfcnpj_2);"))
    }

    cli::cli_alert_success("Índices criados")

  # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
