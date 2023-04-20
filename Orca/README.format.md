# GPOPT and ORCA code formatting

### Tools

1. We are using [clang-format](https://clang.llvm.org/docs/ClangFormat.html).

[clang-format-style-options]: https://clang.llvm.org/docs/ClangFormatStyleOptions.html
[clang-format-style-options.10]: https://releases.llvm.org/10.0.0/tools/clang/docs/ClangFormatStyleOptions.html
[clang-format.10]: https://releases.llvm.org/10.0.0/tools/clang/docs/ClangFormat.html
1. We use the current stable release, with an eye to good new options coming from [the next release][clang-format-style-options].
   As of writing we're using [release 10][clang-format.10],
   the configuration options are [documented here][clang-format-style-options.10].

1. As mentioned in the [style guide](StyleGuide.md), the format style is based on Google C++ style guide,
   but mixed in with Postgres formatting rules (break before braces, 4-space tabbing, etc)

1. We should use an explicit, complete (locked down) specification for the [.clang-format](.clang-format) file.

1. But our intent is better expressed as [well organized, commented yaml](clang-format.intent.yaml).
   We use a [simple script](scripts/fmt) to generate the complete config file from the intent file. For example, on my Linux laptop, I run:

   ```shell
   CLANG_FORMAT=clang-format-10 scripts/fmt gen
   ```

   If the correct version of `clang-format` is installed as `clang-format` (as is the case in macOS), you can omit the environment variable override.

1. To check for formatting conformance, one can run

   ```shell
   scripts/fmt chk
   ```

   It will succeed quietly (with return code 0) or point out the first few places that need to be formatted.

1. To wholesale format all of ORCA and GPOPT

   ```shell
   scripts/fmt fmt
   ```

   On my laptop this takes about 2.5 seconds.

1. Of course, when you're writing code, 2.5 seconds is an eternity.
   Here's a non-exhaustive list of editor / IDE plugins that provide an
   integrated formatting experience.
   The in-editor formatting typically takes around 50ms.
   Depending on your plugin / editor,
   the formatting either happens automatically as you type, on save,
   or when you invoke it explicitly.

   [int.emacs]: https://clang.llvm.org/docs/ClangFormat.html#emacs-integration
   * LLVM's official [Emacs integration][int.emacs] provides the _`clang-format-region`_ macro.

   [int.vim]: https://clang.llvm.org/docs/ClangFormat.html#vim-integration
   * [Vim integration][int.vim] as documented by the official LLVM project

   [int.altvim]: https://github.com/rhysd/vim-clang-format
   * A community [VIM plugin][int.altvim] that looks promising and claims to be better than the official VIM integration

   [int.clion]: https://www.jetbrains.com/help/clion/clangformat-as-alternative-formatter.html
   * [Clion][int.clion] detects `.clang-format` automatically and switches to using it for the built-in "Reformat Code" and "Reformat File...".

   [int.vscode]: https://code.visualstudio.com/docs/cpp/cpp-ide#_code-formatting
   * Similar to CLion, [Visual Studio Code has it built in][int.vscode].

   [int.xcode8]: https://github.com/mapbox/XcodeClangFormat
   * [Plugin for Xcode 8+][int.xcode8]

### Annotating history
1. It's usually desirable to skip past formatting commits when doing a `git blame`. Fortunately, git blame has two options that are extremely helpful:
    1. `-w` ignore whitespace
    1. `--ignore-rev` / `--ignore-revs-file` accepts commits to skip while annotating

  When combined, `-w --ignore-revs-file` produces a perfectly clean annotation, unencumbered by a noisy history. This also gives us a peace of mind in changing our formats more frequently without worrying about hindering the `git blame` experience, rather than "do it once, set it in stone, and forget about it".

### Reformatting in-flight patches
ORCA is actively developed, that means when the "big bang" formatting change is merged,
there will inevitably be a number of patches that were started well before the formatting change,
and these patches will now need to conform to the new format.
The following steps are used to convert in-flight branches to the new format.

1. Get the format script

   ```sh
   git restore --source master -- scripts/fmt
   ```

1. Get the format configuration files

   ```sh
   git restore --source master -- src/include/gpopt/.clang-format src/backend/gpopt/.clang-format src/backend/gporca/.clang-format
   ```

1. Use `git filter-branch` to rewrite the branch history

   ```sh
   git filter-branch --tree-filter 'scripts/fmt fmt' master..
   ```

1. Now that we have reformatted the history, rebase on top of master

   ```sh
   git rebase master
   ```
