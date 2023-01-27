#' @title
#' Importar arquivo de investimentos em CDB, LCI e LCA
#'
#' @description
#' Importa o arquivo CDB para o R, para o DuckDB ou para o Postgres.
#' Esse arquivo é disponibilizado pela Gefin no diretório
#' F:\\USERS\\AUDITEXT\\IFT\\Afb.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.txt).
#' @param import_db
#' String. "none", "duckdb" ou "postgres".
#' @param usuario
#' String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#' String. Se import_db = "postgres", informar o IP do servidor.
#' @param port
#' Int. Se import_db = "postgres", informar a porta do servidor.
#' @param senha
#' String. Se import_db = "postgres", a senha deverá ser informada.
#' @param db
#' String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
#' Se import_db = "postgres", o nome do DB.
#' @param metodo
#' String. "readr" ou "vroom".
#' @param db
#' String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
#' @param tabela
#' String. Nome da tabela a ser criada no DuckDB.
#'
#' @details
#' Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' No caso de importar par ao Postgres será necessário um usuário e uma senha.
#' Além disso, o IP também deve ser autorizado.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_cdb("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_cdb(
#' "\\path\\to\\file.txt",
#' import_db = "duckdb",
#' db = "\\path\\to\\duck.db"),
#' tabela = "cdb"
#' }
#'
#' # para importar para o Postgres:
#' import_cdb(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "cdb")
#'


import_cdb = function(
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

  match.arg(
    arg = metodo,
    choices = c("vroom", "readr"),
    several.ok = FALSE
  )

  # evaluate arg import_db
  match.arg(
    arg = import_db,
    choices = c("none", "duckdb", "postgres"),
    several.ok = FALSE
    )


# conexao com DuckDB

  if (import_db == "duckdb") {

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com DuckDB")
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
    "conta", "pf_ou_pj", "lig_ou_nao", "cpfcnpj", "nome",
    "aplic", "c_cosif", "c_contabil", "dt_aplic", "dt_venc",
    "pre_ou_pos", "taxa", "ind",
    "porcentagem", "valor_aplic", "saldo_aplic",
    "jr_aprov", "agencia", "cid", "n_operador",
    "governo", "inst_fin", "invest_inst",
    "prefeitura", "cod_cetip", "municipio", "uf",
    "bloqueio", "vl_bacenjud"
    )



    larguras = c(
      10, 1, 3, 14, 30, 3, 8, 5, 8, 8, 3, 13, 3, 5, 16, 16, 16, 30, 4, 10, 3, 3, 3, 3, 11, 30, 2, 1, 16
      )

    col_types = readr::cols(
      dt_aplic = readr::col_date("%Y%m%d"),
      dt_venc = readr::col_date("%Y%m%d"),
      valor_aplic = readr::col_double(),
      saldo_aplic = readr::col_double(),
      vl_bacenjud = readr::col_double(),
      c_cosif = readr::col_character()


    )


    # importar arquivo para o R via {readr}
    if (metodo == "readr") {

      cli::cli_alert_info("Iniciando importação com readr")


      data = readr::read_fwf(
        caminho,
        readr::fwf_widths(larguras,
                          colunas, ),
        col_types = col_types,
        skip = 1
      )
    }

    # importar arquivo para o R via {vroom}
    if (metodo == "vroom") {

      cli::cli_alert_info("Iniciando importação com vroom")
      ini = Sys.time()
       data = vroom::vroom_fwf(
        caminho,
        readr::fwf_widths(larguras,
                          colunas),
        col_types = col_types,
        skip = 1
      )

     fim = Sys.time()
     cli::cli_alert_success("Arquivo importado em {(fim - ini) / 60} min")
    }

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
  cli::cli_alert_success("Linhas removidas em {(fim - ini) / 60} min")

  # criar tabela no DuckDB
  if (import_db != "none") {

  # dropar tabela
    DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", tabela))
    cli::cli_alert_success("Tabela dropada")

  # gravar tabela
    ini = as.numeric(format(Sys.time(), "%S")) / 60
    cli::cli_alert_info("Iniciando gravação para o banco de dados")
    DBI::dbWriteTable(con, DBI::SQL(tabela), data)
    fim = as.numeric(format(Sys.time(), "%M")) / 60
    cli::cli_alert_success("Tabela gravada em {(fim - ini)} minutos")

  # gravar índices
  cli::cli_alert_info("Iniciando gravação de índices")

  if (import_db == "duckdb") {
  DBI::dbExecute(con, paste0("
  ALTER TABLE ", tabela, "
  ADD INDEX  (cpfcnpj(14))"
  )
 )
 cli::cli_alert_success("Gravação concluída")
}

  if (import_db == "postgres") {
    DBI::dbExecute(con, paste0("
  CREATE INDEX ON ", tabela, " (cpfcnpj);")
    )
  cli::cli_alert_success("Gravação concluída")
  }

   # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
  remove(con)
  invisible(gc())

  } else {
    data
  }
}