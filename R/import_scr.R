#' @title
#' Importar arquivo da base geral do Banco Central
#'
#' @description
#' Importa o arquivo scr para o R, para o DuckDB ou para o Postgres.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.csv).
#' @param import_db
#' String. "none", "duckdb" ou "postgres".
#' @param usuario
#' String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#' String. Se import_db = "postgres", informar o IP do banco de dados.
#' @param port
#' Int. Se import_db = "postgres", informar a porta.
#' @param senha
#' String. Se import_db = "postgres", a senha deverá ser informada.
#' @param db
#' String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
#' Se import_db = "postgres", o nome do DB.
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
#' data = import_arqtra("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_scr(
#' "\\path\\to\\file.scv",
#' import_db = "duckdb",
#' db = "\\path\\to\\duck.db"),
#' tabela = "scr"
#' }
#'
#' # para importar para o Postgres:
#' import_scr(
#' "\\path\\to\\file.txt"),
#' import_scr = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "scr")
#'

import_scr = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela) {

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

  
  # importar arquivo para o R
  xml = xml2::read_xml(caminho)
   unpack_attr = function(xi, i) {
    attrs = attributes(xi)
    if (length(xi) < 1L || is.null(attrs[["names"]]) ) {
      xi = list(. = NA)
    } else {
      xi = list(. = `attributes<-`(xi, NULL))
    }
    c(xi, `[[<-`(attrs, "names", NULL))
  }

  unpack_attrs = function(x, ids = NULL) {
    out = list()
    i = 1L
    while (i <= length(x)) {
      out[[i]] = unpack_attr(x[[i]])
      i = i + 1L
    }
    names(out) = ids
    rbindlist(out, fill = TRUE, idcol = TRUE)
  }

  recur_unpack_attrs = function(xml_tree) {
    out = unpack_attrs(xml_tree)[, .id := as.character(.I)]
    nms = names(out)[-1:-2]
    out_nms = nms
    while (!all(is.na(out[["."]]))) {
      last = copy(out)
      out = out[, unpack_attrs(., .id)]
      tmp = names(out)
      conf = match(out_nms, tmp, 0L); conf = conf[conf > 0L]
      if (length(conf) > 0L) tmp[conf] = paste0(tmp[conf], "_", conf - 2L + length(out_nms))
      out_nms = c(out_nms, tmp[-1:-2])
      names(out) = tmp
      out[last, (nms) := mget(paste0("i.", nms)), on = ".id"][, .id := as.character(.I)]
      nms = names(out)[-1:-2]
      gc()
    }
    out[rowSums(is.na(out)) < length(out) - 1L, ..out_nms]
  }

  xml_ls = xml2::as_list(xml2::xml_find_all(xml, xpath = "//Cli"))
  index = seq_len(length(xml_ls))
  tasks = split(index, (index - 1L) %/% 50000L)
  data = tibble::as_tibble(rbindlist(lapply(tasks, function(task) recur_unpack_attrs(xml_ls[task])), fill = TRUE))

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
  ADD INDEX (Cd(14))"
  )
 )
 cli::cli_alert_success("Gravação concluída")
}

  if (import_db == "postgres") {
    DBI::dbExecute(con, paste0("
  CREATE INDEX ON ", tabela, " (Cd);")
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

