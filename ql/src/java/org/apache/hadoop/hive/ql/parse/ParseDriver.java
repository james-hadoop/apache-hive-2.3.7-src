/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.Context;

/**
 * ParseDriver.
 *
 */
public class ParseDriver {

  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.parse.ParseDriver");

  /**
   * ANTLRNoCaseStringStream.
   *
   */
  //This class provides and implementation for a case insensitive token checker
  //for the lexical analysis part of antlr. By converting the token stream into
  //upper case at the time when lexical rules are checked, this class ensures that the
  //lexical rules need to just match the token with upper case letters as opposed to
  //combination of upper case and lower case characteres. This is purely used for matching lexical
  //rules. The actual token text is stored in the same way as the user input without
  //actually converting it into an upper case. The token values are generated by the consume()
  //function of the super class ANTLRStringStream. The LA() function is the lookahead funtion
  //and is purely used for matching lexical rules. This also means that the grammar will only
  //accept capitalized tokens in case it is run from other tools like antlrworks which
  //do not have the ANTLRNoCaseStringStream implementation.
  public class ANTLRNoCaseStringStream extends ANTLRStringStream {

    public ANTLRNoCaseStringStream(String input) {
      super(input);
    }

    @Override
    public int LA(int i) {

      int returnChar = super.LA(i);
      if (returnChar == CharStream.EOF) {
        return returnChar;
      } else if (returnChar == 0) {
        return returnChar;
      }

      return Character.toUpperCase((char) returnChar);
    }
  }

  /**
   * HiveLexerX.
   *
   */
  public class HiveLexerX extends HiveLexer {

    private final ArrayList<ParseError> errors;

    public HiveLexerX() {
      super();
      errors = new ArrayList<ParseError>();
    }

    public HiveLexerX(CharStream input) {
      super(input);
      errors = new ArrayList<ParseError>();
    }

    @Override
    public void displayRecognitionError(String[] tokenNames,
        RecognitionException e) {

      errors.add(new ParseError(this, e, tokenNames));
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
      String msg = null;

      if (e instanceof NoViableAltException) {
        @SuppressWarnings("unused")
        NoViableAltException nvae = (NoViableAltException) e;
        // for development, can add
        // "decision=<<"+nvae.grammarDecisionDescription+">>"
        // and "(decision="+nvae.decisionNumber+") and
        // "state "+nvae.stateNumber
        msg = "character " + getCharErrorDisplay(e.c) + " not supported here";
      } else {
        msg = super.getErrorMessage(e, tokenNames);
      }

      return msg;
    }

    public ArrayList<ParseError> getErrors() {
      return errors;
    }

  }

  /**
   * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes
   * so that the graph walking algorithms and the rules framework defined in
   * ql.lib can be used with the AST Nodes.
   */
  public static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    /**
     * Creates an ASTNode for the given token. The ASTNode is a wrapper around
     * antlr's CommonTree class that implements the Node interface.
     *
     * @param payload
     *          The token.
     * @return Object (which is actually an ASTNode) for the token.
     */
    @Override
    public Object create(Token payload) {
      return new ASTNode(payload);
    }

    @Override
    public Object dupNode(Object t) {

      return create(((CommonTree)t).token);
    };

    @Override
    public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
      return new ASTErrorNode(input, start, stop, e);
    };
  };

  public ASTNode parse(String command) throws ParseException {
    return parse(command, null);
  }
  
  public ASTNode parse(String command, Context ctx) 
      throws ParseException {
    return parse(command, ctx, null);
  }

  /**
   * Parses a command, optionally assigning the parser's token stream to the
   * given context.
   *
   * @param command
   *          command to parse
   *
   * @param ctx
   *          context with which to associate this parser's token stream, or
   *          null if either no context is available or the context already has
   *          an existing stream
   *
   * @return parsed AST
   */
  public ASTNode parse(String command, Context ctx, String viewFullyQualifiedName)
      throws ParseException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing command: " + command);
    }

    HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    if (ctx != null) {
      if (viewFullyQualifiedName == null) {
        // Top level query
        ctx.setTokenRewriteStream(tokens);
      } else {
        // It is a view
        ctx.addViewTokenRewriteStream(viewFullyQualifiedName, tokens);
      }
      lexer.setHiveConf(ctx.getConf());
    }
    HiveParser parser = new HiveParser(tokens);
    if (ctx != null) {
      parser.setHiveConf(ctx.getConf());
    }
    parser.setTreeAdaptor(adaptor);
    HiveParser.statement_return r = null;
    try {
      r = parser.statement();
    } catch (RecognitionException e) {
      e.printStackTrace();
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    /** 
     * create by James on 2020-06-05.
     * TODO
     * 
     * Hive SQL解析过程
     * 1. 将SQL转换为ASTNode过程如下（SQL->AST(Abstract Syntax Tree)）
     */
    ASTNode tree = (ASTNode) r.getTree();
    tree.setUnknownTokenBoundaries();
    return tree;
  }

  /*
   * Parse a string as a query hint.
   */
  public ASTNode parseHint(String command) throws ParseException {
    LOG.info("Parsing hint: " + command);

    HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HintParser parser = new HintParser(tokens);
    parser.setTreeAdaptor(adaptor);
    HintParser.hint_return r = null;
    try {
      r = parser.hint();
    } catch (RecognitionException e) {
      e.printStackTrace();
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.info("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }

  /*
   * parse a String as a Select List. This allows table functions to be passed expression Strings
   * that are translated in
   * the context they define at invocation time. Currently used by NPath to allow users to specify
   * what output they want.
   * NPath allows expressions n 'tpath' a column that represents the matched set of rows. This
   * column doesn't exist in
   * the input schema and hence the Result Expression cannot be analyzed by the regular Hive
   * translation process.
   */
  public ASTNode parseSelect(String command, Context ctx) throws ParseException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing command: " + command);
    }

    HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    if (ctx != null) {
      ctx.setTokenRewriteStream(tokens);
    }
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    HiveParser_SelectClauseParser.selectClause_return r = null;
    try {
      r = parser.selectClause();
    } catch (RecognitionException e) {
      e.printStackTrace();
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.debug("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }
  public ASTNode parseExpression(String command) throws ParseException {
    LOG.info("Parsing expression: " + command);

    HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);
    HiveParser_IdentifiersParser.expression_return r = null;
    try {
      r = parser.expression();
    } catch (RecognitionException e) {
      e.printStackTrace();
      throw new ParseException(parser.errors);
    }

    if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
      LOG.info("Parse Completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.errors);
    }

    return (ASTNode) r.getTree();
  }
}
