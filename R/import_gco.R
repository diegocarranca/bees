#' @title
#' Importar arquivo de contratos de credito
#'
#' @description
#' Importa o arquivo GCOA53 para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.txt).
#' @param duckdb Logical.
#' Se TRUE, entao o arquivo sera importado para o DuckDB.
#' @param db String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
#' @param tabela String. Nome da tabela a ser criada no DuckDB.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_gco("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_bg(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "bg_pf"
#' }
#'

import_gco = function(
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
  if (import_db != "none" && is.null(usuario)) {
    stop("usuário deve ser informado")
  }

  # evaluate arg senha
  if (import_db != "none" && is.null(senha)) {
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

  # layout
  larguras = c(
    6, 4, 4, 20, 8, 3, 4, 8, 8, 17, 17, 17, 4, 1, 2, 2, 2, 2, 5, 4, 14,
    30, 13, 1, 1, 2, 8, 20, 2, 1, 1, 13, 11, 2, 2, 2, 2, 17, 9, 1, 4, 1,
    2, 2, 2
  )

  colunas = c(
    "processamento", "produto", "subproduto", "contrato", "clientegco",
    "sistema", "agencia", "efetivacao", "vencimento", "saldodevedor",
    "saldocontabil", "saldopdd", "diasatraso", "adiantamento",
    "classorigem", "class", "classinformada", "classfinal",
    "grupocontabil", "diasprazo", "cpfcnpj", "nome", "vlrutilizadopdd",
    "prejuizo", "tipocredito", "codinterno", "transfprejuizo",
    "tipoperda", "idempresa", "procadm", "procjud", "saldora", "cosif",
    "classggc", "classoperacional", "idpdd", "classrisco", "contratado",
    "txjuros", "tipotx", "indexador", "tipocli", "canalorigem",
    "canalatual", "classoficial"
  )

  col_types = readr::cols(
    tipocli = readr::col_character(),
    adiantamento = readr::col_character(),
    tipoperda = readr::col_character(),
    procadm = readr::col_character(),
    procjud = readr::col_character(),
    classoperacional = readr::col_character(),
    classoficial = readr::col_character(),
    txjuros = readr::col_double(),
    diasatraso = readr::col_double(),
    diasprazo = readr::col_double(),
    vlrutilizadopdd = readr::col_double(),
    contrato = readr::col_character(),
    saldodevedor = readr::col_double(),
    saldocontabil = readr::col_double(),
    saldora = readr::col_double(),
    contratado = readr::col_double(),
    saldopdd = readr::col_double(),
    processamento = readr::col_character(),
    vencimento = readr::col_date("%Y%m%d"),
    transfprejuizo = readr::col_date("%Y%m%d"),
    efetivacao = readr::col_date("%Y%m%d")
  )

 # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(larguras,
                        colunas),
      col_types = col_types,
      skip = 1
    )
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas),
      col_types = col_types,
      skip = 1
    )
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
  cli::cli_alert_success("Linhas removidas em {(fim - ini) / 60} min")

  # criar tabela no DuckDB
  if (import_db != "none") {

    # dropar tabela
    DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", tabela))

    # gravar tabela
    cli::cli_alert_info("Iniciando gravação para o banco de dados")
    DBI::dbWriteTable(con, DBI::SQL(tabela), data)
    cli::cli_alert_success("Gravação concluída")

    # gravar índices
    cli::cli_alert_info("Iniciando gravação de índices")

    if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX idx_contrato (contrato(20)),
      ADD INDEX idx_prejuizo (prejuizo(1)),
      ADD INDEX idx_cpfcnpj (cpfcnpj(14)),
      ADD INDEX idx_agencia (agencia(4)),
      ADD INDEX idx_produto (produto(4)),
      ADD INDEX idx_tipocli (tipocli(1));
      "))
    }

    if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_contrato ON ", tabela, " (contrato);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_prejuizo ON ", tabela, " (prejuizo);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_cpfcnpj ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_agencia ON ", tabela, " (agencia);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_produto ON ", tabela, " (produto);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_tipocli ON ", tabela, " (tipocli);"))
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
